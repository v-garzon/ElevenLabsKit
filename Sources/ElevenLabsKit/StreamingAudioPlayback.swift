import AudioToolbox
@preconcurrency import AVFoundation
import Foundation
import OSLog

final class StreamingAudioPlayback: @unchecked Sendable {
    private static let bufferCount: Int = 3
    private static let bufferSize: Int = 32 * 1024

    private let logger: Logger
    private let lock = NSLock()
    fileprivate let audio: AudioToolboxClient
    private let scheduleParseWork: (@escaping @Sendable () -> Void) -> Void
    fileprivate let bufferLock = NSLock()
    fileprivate let bufferSemaphore = DispatchSemaphore(value: bufferCount)

    private var continuation: CheckedContinuation<StreamingPlaybackResult, Never>?
    private var finished = false

    private var audioFileStream: AudioFileStreamID?
    private var audioQueue: AudioQueueRef?
    fileprivate var audioFormat: AudioStreamBasicDescription?
    fileprivate var maxPacketSize: UInt32 = 0

    fileprivate var availableBuffers: [AudioQueueBufferRef] = []
    private var currentBuffer: AudioQueueBufferRef?
    private var currentBufferSize: Int = 0
    private var currentPacketDescs: [AudioStreamPacketDescription] = []

    fileprivate var inputFinished = false
    private var startRequested = false

    private var sampleRate: Double = 0

    // MARK: — Shared engine path (AVAudioPlayerNode)

    /// Non-nil when using a shared AVAudioEngine instead of AudioQueue.
    private let sharedEngine: AVAudioEngine?

    /// AVAudioPlayerNode attached to sharedEngine. Created in setupPlayerNodeIfNeeded,
    /// stays attached for the lifetime of this instance, detached in teardown.
    private var playerNode: AVAudioPlayerNode?

    /// Converter from compressed MP3 format to PCM float32.
    /// All access is on the parse queue — no lock needed.
    private var audioConverter: AVAudioConverter?

    /// PCM output format used by the converter and playerNode connection.
    private var pcmOutputFormat: AVAudioFormat?

    /// Number of PCM buffers scheduled on playerNode but whose completion has not yet fired.
    /// Incremented on the parse queue, decremented on the AVAudioEngine callback thread.
    /// Always accessed under `lock`.
    private var pendingNodeBuffers: Int = 0

    /// True once playerNode.play() has been called. Only accessed on the parse queue.
    private var playerNodeStarted: Bool = false

    // MARK: — Inits

    /// Default init — uses AudioQueue for playback (original behavior unchanged).
    init(
        logger: Logger,
        audio: AudioToolboxClient = .live,
        scheduleParseWork: ((@escaping @Sendable () -> Void) -> Void)? = nil
    ) {
        self.logger = logger
        self.audio = audio
        self.sharedEngine = nil
        if let scheduleParseWork {
            self.scheduleParseWork = scheduleParseWork
        } else {
            let parseQueue = DispatchQueue(label: "talk.stream.parse")
            self.scheduleParseWork = { work in parseQueue.async(execute: work) }
        }
    }

    /// Shared engine init — uses AVAudioPlayerNode on the provided engine for playback.
    /// AudioQueue is never created. The engine is never stopped by this instance.
    init(
        logger: Logger,
        sharedEngine: AVAudioEngine,
        audio: AudioToolboxClient = .live,
        scheduleParseWork: ((@escaping @Sendable () -> Void) -> Void)? = nil
    ) {
        self.logger = logger
        self.audio = audio
        self.sharedEngine = sharedEngine
        if let scheduleParseWork {
            self.scheduleParseWork = scheduleParseWork
        } else {
            let parseQueue = DispatchQueue(label: "talk.stream.parse")
            self.scheduleParseWork = { work in parseQueue.async(execute: work) }
        }
    }

    func setContinuation(_ continuation: CheckedContinuation<StreamingPlaybackResult, Never>) {
        lock.lock()
        self.continuation = continuation
        lock.unlock()
    }

    func start() {
        let selfPtr = Unmanaged.passUnretained(self).toOpaque()
        let status = audio.fileStreamOpen(
            selfPtr,
            propertyListenerProc,
            packetsProc,
            kAudioFileMP3Type,
            &audioFileStream
        )
        if status != noErr {
            logger.error("talk stream open failed: \(status)")
            finish(StreamingPlaybackResult(finished: false, interruptedAt: nil))
        }
    }

    func append(_ data: Data) {
        guard !data.isEmpty else { return }
        scheduleParseWork { [weak self] in
            guard let self else { return }
            guard let audioFileStream else { return }
            let status = data.withUnsafeBytes { bytes in
                audio.fileStreamParseBytes(
                    audioFileStream,
                    UInt32(bytes.count),
                    bytes.baseAddress,
                    []
                )
            }
            if status != noErr {
                logger.error("talk stream parse failed: \(status)")
                fail(NSError(domain: "StreamingAudio", code: Int(status)))
            }
        }
    }

    func finishInput() {
        scheduleParseWork { [weak self] in
            guard let self else { return }

            // — Shared engine path —
            if sharedEngine != nil {
                // Set inputFinished under lock so the completion callback sees it safely.
                lock.lock()
                inputFinished = true
                let pending = pendingNodeBuffers
                lock.unlock()

                if pending == 0 {
                    // No buffers were ever scheduled (e.g. empty stream or setup failure).
                    finish(StreamingPlaybackResult(finished: playerNode != nil, interruptedAt: nil))
                }
                // If pending > 0, the last completion callback will call finish.
                return
            }

            // — AudioQueue path (original) —
            inputFinished = true
            if audioQueue == nil {
                finish(StreamingPlaybackResult(finished: false, interruptedAt: nil))
                return
            }
            enqueueCurrentBuffer(flushOnly: true)
            _ = stop(immediate: false)
        }
    }

    func fail(_ error: Error) {
        logger.error("talk stream failed: \(error.localizedDescription, privacy: .public)")
        _ = stop(immediate: true)
        finish(StreamingPlaybackResult(finished: false, interruptedAt: nil))
    }

    func stop(immediate: Bool) -> Double? {
        // — Shared engine path —
        if let playerNode {
            let interruptedAt = currentTimeSecondsForNode(playerNode)
            playerNode.stop()
            // Reset counter so any in-flight completion callbacks don't re-trigger finish.
            lock.lock()
            pendingNodeBuffers = 0
            lock.unlock()
            return interruptedAt
        }

        // — AudioQueue path (original) —
        guard let audioQueue else { return nil }
        let interruptedAt = currentTimeSeconds()
        _ = audio.queueStop(audioQueue, immediate)
        return interruptedAt
    }

    func finish(_ result: StreamingPlaybackResult) {
        let continuation: CheckedContinuation<StreamingPlaybackResult, Never>?
        lock.lock()
        if finished {
            continuation = nil
        } else {
            finished = true
            continuation = self.continuation
            self.continuation = nil
        }
        lock.unlock()

        continuation?.resume(returning: result)
        teardown()
    }

    private func teardown() {
        // — Shared engine path —
        if let engine = sharedEngine, let node = playerNode {
            node.stop()
            engine.detach(node)
            playerNode = nil
            audioConverter = nil
            pcmOutputFormat = nil
        }

        // — AudioQueue path (no-op if audioQueue is nil) —
        if let audioQueue {
            _ = audio.queueDispose(audioQueue, true)
            self.audioQueue = nil
        }

        // Always clean up file stream and buffer state.
        if let audioFileStream {
            _ = audio.fileStreamClose(audioFileStream)
            self.audioFileStream = nil
        }
        bufferLock.lock()
        availableBuffers.removeAll()
        bufferLock.unlock()
        currentBuffer = nil
        currentPacketDescs.removeAll()
    }

    // MARK: — Queue / PlayerNode setup

    func setupQueueIfNeeded(_ asbd: AudioStreamBasicDescription) {
        // Dispatch to appropriate setup path.
        if sharedEngine != nil {
            setupPlayerNodeIfNeeded(asbd)
            return
        }

        // — AudioQueue path (original) —
        guard audioQueue == nil else { return }

        var format = asbd
        audioFormat = format
        sampleRate = format.mSampleRate

        let selfPtr = Unmanaged.passUnretained(self).toOpaque()
        let status = audio.queueNewOutput(
            &format,
            outputCallbackProc,
            selfPtr,
            nil,
            nil,
            0,
            &audioQueue
        )
        if status != noErr {
            logger.error("talk queue create failed: \(status)")
            finish(StreamingPlaybackResult(finished: false, interruptedAt: nil))
            return
        }

        if let audioQueue {
            _ = audio.queueAddPropertyListener(audioQueue, kAudioQueueProperty_IsRunning, isRunningCallbackProc, selfPtr)
        }

        if let audioFileStream {
            var cookieSize: UInt32 = 0
            var writable: DarwinBoolean = false
            let cookieStatus = audio.fileStreamGetPropertyInfo(
                audioFileStream,
                kAudioFileStreamProperty_MagicCookieData,
                &cookieSize,
                &writable
            )
            if cookieStatus == noErr, cookieSize > 0, let audioQueue {
                var cookie = [UInt8](repeating: 0, count: Int(cookieSize))
                let readStatus = cookie.withUnsafeMutableBytes { bytes -> OSStatus in
                    guard let base = bytes.baseAddress else { return -1 }
                    return audio.fileStreamGetProperty(
                        audioFileStream,
                        kAudioFileStreamProperty_MagicCookieData,
                        &cookieSize,
                        base
                    )
                }
                if readStatus == noErr {
                    _ = audio.queueSetProperty(audioQueue, kAudioQueueProperty_MagicCookie, cookie, cookieSize)
                }
            }
        }

        if let audioQueue {
            for _ in 0..<Self.bufferCount {
                var buffer: AudioQueueBufferRef?
                let allocStatus = audio.queueAllocateBuffer(audioQueue, UInt32(Self.bufferSize), &buffer)
                if allocStatus == noErr, let buffer {
                    bufferLock.lock()
                    availableBuffers.append(buffer)
                    bufferLock.unlock()
                }
            }
        }
    }

    /// Sets up AVAudioPlayerNode and AVAudioConverter for the shared engine path.
    /// Called at most once per instance (guarded by playerNode == nil check).
    /// Runs on the parse queue.
    private func setupPlayerNodeIfNeeded(_ asbd: AudioStreamBasicDescription) {
        guard playerNode == nil, let sharedEngine else { return }

        var mutableASBD = asbd
        audioFormat = mutableASBD
        sampleRate = mutableASBD.mSampleRate

        // Build input format from the compressed stream description.
        guard let inputFormat = AVAudioFormat(streamDescription: &mutableASBD) else {
            logger.error("talk player node: failed to create input format from ASBD")
            finish(StreamingPlaybackResult(finished: false, interruptedAt: nil))
            return
        }

        // PCM float32 output at the same sample rate and channel count.
        let channels = AVAudioChannelCount(max(1, mutableASBD.mChannelsPerFrame))
        guard let outputFormat = AVAudioFormat(
            standardFormatWithSampleRate: mutableASBD.mSampleRate,
            channels: channels
        ) else {
            logger.error("talk player node: failed to create PCM output format")
            finish(StreamingPlaybackResult(finished: false, interruptedAt: nil))
            return
        }

        guard let converter = AVAudioConverter(from: inputFormat, to: outputFormat) else {
            logger.error("talk player node: failed to create AVAudioConverter (MP3 → PCM)")
            finish(StreamingPlaybackResult(finished: false, interruptedAt: nil))
            return
        }

        let node = AVAudioPlayerNode()
        sharedEngine.attach(node)
        sharedEngine.connect(node, to: sharedEngine.mainMixerNode, format: outputFormat)

        // Start engine if the caller has not done so yet.
        if !sharedEngine.isRunning {
            do {
                try sharedEngine.start()
            } catch {
                logger.error("talk player node: engine start failed: \(error.localizedDescription, privacy: .public)")
                sharedEngine.detach(node)
                finish(StreamingPlaybackResult(finished: false, interruptedAt: nil))
                return
            }
        }

        playerNode = node
        audioConverter = converter
        pcmOutputFormat = outputFormat
    }

    // MARK: — Packet handling

    private func enqueueCurrentBuffer(flushOnly: Bool = false) {
        guard let audioQueue, let buffer = currentBuffer else { return }
        guard currentBufferSize > 0 else { return }

        buffer.pointee.mAudioDataByteSize = UInt32(currentBufferSize)
        let packetCount = UInt32(currentPacketDescs.count)

        let status = currentPacketDescs.withUnsafeBufferPointer { descPtr in
            audio.queueEnqueueBuffer(audioQueue, buffer, packetCount, descPtr.baseAddress)
        }
        if status != noErr {
            logger.error("talk queue enqueue failed: \(status)")
        } else {
            if !startRequested {
                startRequested = true
                let startStatus = audio.queueStart(audioQueue, nil)
                if startStatus != noErr {
                    logger.error("talk queue start failed: \(startStatus)")
                }
            }
        }

        currentBuffer = nil
        currentBufferSize = 0
        currentPacketDescs.removeAll(keepingCapacity: true)
        if !flushOnly {
            bufferLock.lock()
            var next = availableBuffers.popLast()
            bufferLock.unlock()
            if next == nil {
                bufferSemaphore.wait()
                bufferLock.lock()
                next = availableBuffers.popLast()
                bufferLock.unlock()
            }
            if let next { currentBuffer = next }
        }
    }

    func handlePackets(
        numberBytes: UInt32,
        numberPackets: UInt32,
        inputData: UnsafeRawPointer,
        packetDescriptions: UnsafeMutablePointer<AudioStreamPacketDescription>?
    ) {
        // — Shared engine path —
        if sharedEngine != nil {
            handlePacketsWithPlayerNode(
                numberBytes: numberBytes,
                numberPackets: numberPackets,
                inputData: inputData,
                packetDescriptions: packetDescriptions
            )
            return
        }

        // — AudioQueue path (original) —
        if audioQueue == nil, let format = audioFormat {
            setupQueueIfNeeded(format)
        }

        if audioQueue == nil {
            return
        }

        if currentBuffer == nil {
            bufferLock.lock()
            currentBuffer = availableBuffers.popLast()
            bufferLock.unlock()
            if currentBuffer == nil {
                bufferSemaphore.wait()
                bufferLock.lock()
                currentBuffer = availableBuffers.popLast()
                bufferLock.unlock()
            }
            currentBufferSize = 0
            currentPacketDescs.removeAll(keepingCapacity: true)
        }

        let bytes = inputData.assumingMemoryBound(to: UInt8.self)
        let packetCount = Int(numberPackets)
        for index in 0..<packetCount {
            let packetOffset: Int
            let packetSize: Int

            if let packetDescriptions {
                packetOffset = Int(packetDescriptions[index].mStartOffset)
                packetSize = Int(packetDescriptions[index].mDataByteSize)
            } else {
                let size = Int(numberBytes) / packetCount
                packetOffset = index * size
                packetSize = size
            }

            if packetSize > Self.bufferSize {
                continue
            }

            if currentBufferSize + packetSize > Self.bufferSize {
                enqueueCurrentBuffer()
            }

            guard let buffer = currentBuffer else { continue }
            let dest = buffer.pointee.mAudioData.advanced(by: currentBufferSize)
            memcpy(dest, bytes.advanced(by: packetOffset), packetSize)

            let desc = AudioStreamPacketDescription(
                mStartOffset: Int64(currentBufferSize),
                mVariableFramesInPacket: 0,
                mDataByteSize: UInt32(packetSize)
            )
            currentPacketDescs.append(desc)
            currentBufferSize += packetSize
        }
    }

    /// Decodes a batch of compressed MP3 packets to PCM and schedules them on the playerNode.
    /// All access is on the parse queue (serial) — no additional locking needed for
    /// audioConverter, pcmOutputFormat, or playerNode itself.
    private func handlePacketsWithPlayerNode(
        numberBytes: UInt32,
        numberPackets: UInt32,
        inputData: UnsafeRawPointer,
        packetDescriptions: UnsafeMutablePointer<AudioStreamPacketDescription>?
    ) {
        guard numberPackets > 0, numberBytes > 0 else { return }

        // Lazy setup: converter + playerNode created on first packet batch.
        if playerNode == nil, let format = audioFormat {
            setupPlayerNodeIfNeeded(format)
        }

        guard let converter = audioConverter,
              let outputFormat = pcmOutputFormat,
              let playerNode else { return }

        // Upper bound: 4096 is safe for MP3 (max ~1441 bytes), 2048 fallback is fine too.
        let effectiveMaxPacketSize = maxPacketSize > 0 ? Int(maxPacketSize) : 4096

        let compressedBuffer = AVAudioCompressedBuffer(
            format: converter.inputFormat,
            packetCapacity: AVAudioPacketCount(numberPackets),
            maximumPacketSize: effectiveMaxPacketSize
        )

        // Copy compressed bytes into the buffer.
        compressedBuffer.byteLength = numberBytes
        compressedBuffer.packetCount = numberPackets
        memcpy(compressedBuffer.data, inputData, Int(numberBytes))

        // Copy or synthesize packet descriptions.
        if let dstDescs = compressedBuffer.packetDescriptions {
            if let srcDescs = packetDescriptions {
                // VBR: use the descriptions provided by AudioFileStream.
                memcpy(dstDescs, srcDescs, Int(numberPackets) * MemoryLayout<AudioStreamPacketDescription>.size)
            } else {
                // CBR: all packets are equal-sized — synthesize descriptions.
                let packetSize = Int(numberBytes) / Int(numberPackets)
                for i in 0..<Int(numberPackets) {
                    dstDescs[i] = AudioStreamPacketDescription(
                        mStartOffset: Int64(i * packetSize),
                        mVariableFramesInPacket: 0,
                        mDataByteSize: UInt32(packetSize)
                    )
                }
            }
        }

        // Allocate output buffer.  mFramesPerPacket is 1152 for standard MP3.
        let rawFPP = converter.inputFormat.streamDescription.pointee.mFramesPerPacket
        let framesPerPacket: AVAudioFrameCount = rawFPP > 0 ? AVAudioFrameCount(rawFPP) : 1152
        let outputFrameCapacity = framesPerPacket * AVAudioFrameCount(numberPackets)

        guard outputFrameCapacity > 0,
              let outputBuffer = AVAudioPCMBuffer(pcmFormat: outputFormat, frameCapacity: outputFrameCapacity) else {
            logger.error("talk player node: failed to allocate PCM output buffer")
            return
        }

        // Decode.  We provide all input in one shot via the input block.
        // The block signals .noDataNow on the second call to stop the converter loop.
        // Use a reference-type wrapper so the Sendable closure can mutate the flag
        // without a compiler warning. The converter calls this block synchronously
        // on the same thread — there is no actual concurrency here.
        final class InputGate: @unchecked Sendable { var provided = false }
        let gate = InputGate()
        var conversionError: NSError?
        let status = converter.convert(to: outputBuffer, error: &conversionError) { _, outStatus in
            if !gate.provided {
                gate.provided = true
                outStatus.pointee = .haveData
                return compressedBuffer
            }
            outStatus.pointee = .noDataNow
            return nil
        }

        switch status {
        case .error:
            logger.error("talk player node: conversion failed: \(conversionError?.localizedDescription ?? "unknown", privacy: .public)")
            return
        case .haveData, .inputRanDry, .endOfStream:
            break
        @unknown default:
            break
        }

        guard outputBuffer.frameLength > 0 else { return }

        // Schedule on the player node.
        lock.lock()
        pendingNodeBuffers += 1
        lock.unlock()

        playerNode.scheduleBuffer(outputBuffer) { [weak self] in
            guard let self else { return }
            self.lock.lock()
            // Guard against going negative if stop() raced and reset the counter.
            if self.pendingNodeBuffers > 0 {
                self.pendingNodeBuffers -= 1
            }
            let pending = self.pendingNodeBuffers
            let done = self.inputFinished
            self.lock.unlock()

            if done, pending == 0 {
                self.finish(StreamingPlaybackResult(finished: true, interruptedAt: nil))
            }
        }

        // Start the player node on the first scheduled buffer.
        if !playerNodeStarted {
            playerNodeStarted = true
            playerNode.play()
        }
    }

    // MARK: — Timestamps

    private func currentTimeSeconds() -> Double? {
        guard let audioQueue, sampleRate > 0 else { return nil }
        var timeStamp = AudioTimeStamp()
        let status = audio.queueGetCurrentTime(audioQueue, nil, &timeStamp, nil)
        if status != noErr { return nil }
        if timeStamp.mSampleTime.isNaN { return nil }
        return timeStamp.mSampleTime / sampleRate
    }

    /// Returns current playback position in seconds for an AVAudioPlayerNode.
    /// Uses lastRenderTime (the most recent render cycle) as the reference point.
    /// Returns nil if the node has not yet started rendering.
    private func currentTimeSecondsForNode(_ node: AVAudioPlayerNode) -> Double? {
        guard let lastRenderTime = node.lastRenderTime,
              lastRenderTime.isSampleTimeValid,
              let playerTime = node.playerTime(forNodeTime: lastRenderTime) else { return nil }
        let outputSampleRate = node.outputFormat(forBus: 0).sampleRate
        guard outputSampleRate > 0 else { return nil }
        return Double(playerTime.sampleTime) / outputSampleRate
    }
}

func propertyListenerProc(
    inClientData: UnsafeMutableRawPointer,
    inAudioFileStream: AudioFileStreamID,
    inPropertyID: AudioFileStreamPropertyID,
    ioFlags _: UnsafeMutablePointer<AudioFileStreamPropertyFlags>
) {
    let playback = Unmanaged<StreamingAudioPlayback>.fromOpaque(inClientData).takeUnretainedValue()

    if inPropertyID == kAudioFileStreamProperty_DataFormat {
        var format = AudioStreamBasicDescription()
        var size = UInt32(MemoryLayout<AudioStreamBasicDescription>.size)
        let status = playback.audio.fileStreamGetProperty(inAudioFileStream, inPropertyID, &size, &format)
        if status == noErr {
            playback.audioFormat = format
            playback.setupQueueIfNeeded(format)
        }
    } else if inPropertyID == kAudioFileStreamProperty_PacketSizeUpperBound {
        var maxPacketSize: UInt32 = 0
        var size = UInt32(MemoryLayout<UInt32>.size)
        let status = playback.audio.fileStreamGetProperty(inAudioFileStream, inPropertyID, &size, &maxPacketSize)
        if status == noErr {
            playback.maxPacketSize = maxPacketSize
        }
    }
}

func packetsProc(
    inClientData: UnsafeMutableRawPointer,
    inNumberBytes: UInt32,
    inNumberPackets: UInt32,
    inInputData: UnsafeRawPointer,
    inPacketDescriptions: UnsafeMutablePointer<AudioStreamPacketDescription>?
) {
    let playback = Unmanaged<StreamingAudioPlayback>.fromOpaque(inClientData).takeUnretainedValue()
    playback.handlePackets(
        numberBytes: inNumberBytes,
        numberPackets: inNumberPackets,
        inputData: inInputData,
        packetDescriptions: inPacketDescriptions
    )
}

func outputCallbackProc(
    inUserData: UnsafeMutableRawPointer?,
    inAQ _: AudioQueueRef,
    inBuffer: AudioQueueBufferRef
) {
    guard let inUserData else { return }
    let playback = Unmanaged<StreamingAudioPlayback>.fromOpaque(inUserData).takeUnretainedValue()
    playback.bufferLock.lock()
    playback.availableBuffers.append(inBuffer)
    playback.bufferLock.unlock()
    playback.bufferSemaphore.signal()
}

func isRunningCallbackProc(
    inUserData: UnsafeMutableRawPointer?,
    inAQ: AudioQueueRef,
    inID: AudioQueuePropertyID
) {
    guard let inUserData else { return }
    guard inID == kAudioQueueProperty_IsRunning else { return }

    let playback = Unmanaged<StreamingAudioPlayback>.fromOpaque(inUserData).takeUnretainedValue()
    var running: UInt32 = 0
    var size = UInt32(MemoryLayout<UInt32>.size)
    let status = playback.audio.queueGetProperty(inAQ, kAudioQueueProperty_IsRunning, &running, &size)
    if status != noErr { return }

    if running == 0, playback.inputFinished {
        playback.finish(StreamingPlaybackResult(finished: true, interruptedAt: nil))
    }
}
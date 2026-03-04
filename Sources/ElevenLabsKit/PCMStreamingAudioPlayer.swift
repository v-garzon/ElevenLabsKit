@preconcurrency import AVFoundation
import Foundation
import OSLog

/// Plays 16-bit PCM streaming audio using AVAudioEngine.
@MainActor
public final class PCMStreamingAudioPlayer {
    /// Shared PCM player instance.
    public static let shared = PCMStreamingAudioPlayer()

    private let logger = Logger(subsystem: "com.steipete.clawdis", category: "talk.tts.pcm")
    private let playerFactory: () -> PCMPlayerNodeing
    private let engineFactory: () -> AVAudioEngine
    private let startEngine: (AVAudioEngine) throws -> Void
    private let stopEngine: (AVAudioEngine) -> Void
    private let isSharedEngine: Bool
    private var engine: AVAudioEngine
    private var player: PCMPlayerNodeing
    private var format: AVAudioFormat?
    private var pendingBuffers: Int = 0
    private var inputFinished = false
    private var continuation: CheckedContinuation<StreamingPlaybackResult, Never>?

    /// Creates a default PCM player with its own private AVAudioEngine.
    public init() {
        self.playerFactory = { AVAudioPlayerNodeAdapter() }
        self.engineFactory = { AVAudioEngine() }
        self.startEngine = { engine in try engine.start() }
        self.stopEngine = { engine in engine.stop() }
        self.isSharedEngine = false
        self.engine = engineFactory()
        self.player = playerFactory()
        player.attach(to: engine)
    }

    /// Creates a PCM player that routes audio through a shared AVAudioEngine.
    ///
    /// The engine is never stopped or recreated by this player — the caller
    /// owns the engine lifecycle. Use this when AEC (setVoiceProcessingEnabled)
    /// is required, since AEC needs both mic input and speaker output on the
    /// same engine instance.
    ///
    /// The player node is attached to the engine once in this init and reused
    /// across all play sessions. It is never detached during normal operation.
    public init(sharedEngine: AVAudioEngine) {
        self.playerFactory = { AVAudioPlayerNodeAdapter() }
        self.engineFactory = { sharedEngine }
        self.startEngine = { engine in
            // Only start if not already running — caller may have started it
            if !engine.isRunning {
                try engine.start()
            }
        }
        // Never stop the shared engine — the caller owns its lifecycle
        self.stopEngine = { _ in }
        self.isSharedEngine = true
        self.engine = sharedEngine
        self.player = AVAudioPlayerNodeAdapter()
        // Attach the player node once — it stays attached for the lifetime
        // of this instance. configure() reconnects it with the correct format
        // before each play session.
        player.attach(to: sharedEngine)
    }

    init(
        playerFactory: @escaping () -> PCMPlayerNodeing,
        engineFactory: @escaping () -> AVAudioEngine,
        startEngine: @escaping (AVAudioEngine) throws -> Void,
        stopEngine: @escaping (AVAudioEngine) -> Void
    ) {
        self.playerFactory = playerFactory
        self.engineFactory = engineFactory
        self.startEngine = startEngine
        self.stopEngine = stopEngine
        self.isSharedEngine = false
        self.engine = engineFactory()
        self.player = playerFactory()
        player.attach(to: engine)
    }

    /// Starts playing PCM data at the provided sample rate.
    public func play(stream: AsyncThrowingStream<Data, Error>, sampleRate: Double) async -> StreamingPlaybackResult {
        stopInternal()

        let format = AVAudioFormat(
            commonFormat: .pcmFormatInt16,
            sampleRate: sampleRate,
            channels: 1,
            interleaved: true
        )

        guard let format else {
            return StreamingPlaybackResult(finished: false, interruptedAt: nil)
        }
        configure(format: format)

        return await withCheckedContinuation { continuation in
            self.continuation = continuation
            self.pendingBuffers = 0
            self.inputFinished = false

            Task { @MainActor [weak self] in
                guard let self else { return }
                do {
                    for try await chunk in stream {
                        await enqueuePCM(chunk, format: format)
                    }
                    finishInput()
                } catch {
                    fail(error)
                }
            }
        }
    }

    /// Stops playback immediately and returns the interrupted timestamp.
    public func stop() -> Double? {
        let interruptedAt = currentTimeSeconds()
        stopInternal()
        finish(StreamingPlaybackResult(finished: false, interruptedAt: interruptedAt))
        return interruptedAt
    }

    private func configure(format: AVAudioFormat) {
        let formatChanged = self.format?.sampleRate != format.sampleRate
            || self.format?.commonFormat != format.commonFormat

        if formatChanged {
            if isSharedEngine {
                // For shared engine: stop the current player node and reconnect
                // with the new format. We never recreate the engine or the node —
                // the same node (attached once in init) is reused.
                // AVAudioEngine.connect handles format changes on an already-attached
                // node gracefully by disconnecting and reconnecting.
                player.stop()
            } else {
                // For private engine: full teardown and recreation, same as original.
                stopEngine(engine)
                engine = engineFactory()
                player = playerFactory()
                player.attach(to: engine)
            }
        }

        self.format = format
        player.connect(to: engine, format: format)
    }

    private func enqueuePCM(_ data: Data, format: AVAudioFormat) async {
        guard !data.isEmpty else { return }
        let frameCount = data.count / MemoryLayout<Int16>.size
        guard frameCount > 0 else { return }
        guard let buffer = AVAudioPCMBuffer(pcmFormat: format, frameCapacity: AVAudioFrameCount(frameCount)) else {
            return
        }
        buffer.frameLength = AVAudioFrameCount(frameCount)

        data.withUnsafeBytes { raw in
            guard let src = raw.baseAddress else { return }
            let audioBuffer = buffer.audioBufferList.pointee.mBuffers
            if let dst = audioBuffer.mData {
                memcpy(dst, src, frameCount * MemoryLayout<Int16>.size)
            }
        }

        pendingBuffers += 1
        Task { @MainActor [weak self] in
            guard let self else { return }
            await player.scheduleBuffer(buffer)
            pendingBuffers = max(0, pendingBuffers - 1)
            if inputFinished, pendingBuffers == 0 {
                finish(StreamingPlaybackResult(finished: true, interruptedAt: nil))
            }
        }

        if !player.isPlaying {
            do {
                try startEngine(engine)
                player.play()
            } catch {
                logger.error("pcm engine start failed: \(error.localizedDescription, privacy: .public)")
                fail(error)
            }
        }
    }

    private func finishInput() {
        inputFinished = true
        if pendingBuffers == 0 {
            finish(StreamingPlaybackResult(finished: true, interruptedAt: nil))
        }
    }

    private func fail(_ error: Error) {
        logger.error("pcm stream failed: \(error.localizedDescription, privacy: .public)")
        finish(StreamingPlaybackResult(finished: false, interruptedAt: nil))
    }

    private func stopInternal() {
        player.stop()
        // For shared engine: stopEngine is a no-op — engine stays running.
        // For private engine: stopEngine stops it as before.
        stopEngine(engine)
        pendingBuffers = 0
        inputFinished = false
    }

    private func finish(_ result: StreamingPlaybackResult) {
        let continuation = continuation
        self.continuation = nil
        continuation?.resume(returning: result)
    }

    private func currentTimeSeconds() -> Double? {
        player.currentTimeSeconds()
    }
}

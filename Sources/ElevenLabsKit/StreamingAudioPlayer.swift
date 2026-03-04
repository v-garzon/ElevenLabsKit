import AVFoundation
import Foundation
import OSLog

/// Playback result for a streaming audio session.
public struct StreamingPlaybackResult: Sendable {
    /// True when playback completed without interruption.
    public let finished: Bool
    /// Timestamp in seconds where playback stopped, if interrupted.
    public let interruptedAt: Double?

    /// Creates a playback result.
    public init(finished: Bool, interruptedAt: Double?) {
        self.finished = finished
        self.interruptedAt = interruptedAt
    }
}

/// Plays streaming audio chunks using the shared AVAudioSession-backed player.
@MainActor
public final class StreamingAudioPlayer: NSObject {
    /// Shared player instance.
    public static let shared = StreamingAudioPlayer()

    private let logger = Logger(subsystem: "com.steipete.clawdis", category: "talk.tts.stream")
    private var playback: StreamingAudioPlayback?

    /// Non-nil when routing through a shared AVAudioEngine (AEC path).
    private let sharedEngine: AVAudioEngine?

    /// Creates a player that owns its own AudioQueue (original behavior unchanged).
    override public init() {
        self.sharedEngine = nil
        super.init()
    }

    /// Creates a player that routes audio through a shared AVAudioEngine.
    ///
    /// Use this when AEC (setVoiceProcessingEnabled) is required — AEC needs
    /// both mic input and speaker output on the same engine instance.
    /// The engine is never stopped by this player; the caller owns its lifecycle.
    public init(sharedEngine: AVAudioEngine) {
        self.sharedEngine = sharedEngine
        super.init()
    }

    /// Starts playing a streaming audio payload.
    public func play(stream: AsyncThrowingStream<Data, Error>) async -> StreamingPlaybackResult {
        stopInternal()

        let playback: StreamingAudioPlayback
        if let sharedEngine {
            playback = StreamingAudioPlayback(logger: logger, sharedEngine: sharedEngine)
        } else {
            playback = StreamingAudioPlayback(logger: logger)
        }
        self.playback = playback

        return await withCheckedContinuation { continuation in
            playback.setContinuation(continuation)
            playback.start()

            Task.detached {
                do {
                    for try await chunk in stream {
                        playback.append(chunk)
                    }
                    playback.finishInput()
                } catch {
                    playback.fail(error)
                }
            }
        }
    }

    /// Stops playback immediately and returns the interrupted timestamp.
    public func stop() -> Double? {
        guard let playback else { return nil }
        let interruptedAt = playback.stop(immediate: true)
        finish(playback: playback, result: StreamingPlaybackResult(finished: false, interruptedAt: interruptedAt))
        return interruptedAt
    }

    private func stopInternal() {
        guard let playback else { return }
        let interruptedAt = playback.stop(immediate: true)
        finish(playback: playback, result: StreamingPlaybackResult(finished: false, interruptedAt: interruptedAt))
    }

    private func finish(playback: StreamingAudioPlayback, result: StreamingPlaybackResult) {
        playback.finish(result)
        guard self.playback === playback else { return }
        self.playback = nil
    }
}

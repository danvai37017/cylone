# Cylone, Modified TurboWarp GUI

Cylone is an independent project built using TurboWarp's open-source Scratch GUI.

This repo contains the modified TurboWarp frontend used by Cylone.

## Changes

Cylone modifies `src/lib/cloud-provider.js` to support:

- WebRTC peer-to-peer data channels for cloud-variable updates.
- WebSocket signaling for establishing peer connections.
- WebSocket-based cloud-variable communication when peer data channels are unavailable.
- Peer connection management and reconnection handling.

## Original Project

This project is based on [TurboWarp Scratch GUI](https://github.com/TurboWarp/scratch-gui).

TurboWarp provides the Scratch-compatible compiler, runtime interface, and performance enhancements used by Cylone.

## License

The modified Scratch GUI remains licensed under the GNU General Public License version 3 (GPL-3.0).

See [LICENSE](LICENSE) for the full license text.

Cylone is independent and is not affiliated with or endorsed by TurboWarp, Scratch, or the Scratch Foundation.
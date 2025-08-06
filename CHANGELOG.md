# Revision history for erebos

## 0.2.0 -- 2025-08-06

* Weak references in records
* Use XDG data directory for default storage path
* Added `/identity` command to show details of current identity
* Support tunnel for peers in discovery service
* New CLI prompt implementation providing cleaner interface
    * Avoids displaying sent messages twice – both in previous prompt and in message history
    * Print received messages only for selected conversation
    * Clear tab completion options after use

* API
    * Split `Erebos.Storage` into multiple modules
    * Removed deprecated `Message.formatMessage` alias
    * Renamed `Erebos.Message` module to `Erebos.DirectMessage`
    * Added `StorageBackend` type class to allow custom storage implementation
    * `MonadError` constraints use generic error type
    * Replaced `Erebos.Network.peerAddress` with `getPeerAddress` and added `getPeerAddresses`
    * Renamed `Erebos.Network.peerIdentity` to `getPeerIdentity`
    * Renamed some functions in `Erebos.DirectMessage` module to make clear they are related only to direct messages
    * `Erebos.Storage.Merge.generations`/`generationsBy` return `NonEmpty`
    * Replaced `watchReceivedDirectMessages` with `watchDirectMessageThreads`
    * Return type of `sendMessage` and `sendDirectMessage` is now `()`
    * Some functions use `MonadStorage` instead of explicit `Storage` parameter:
        * `Erebos.Set.storeSetAdd`
        * `Erebos.State.makeSharedStateUpdate`
        * `Erebos.Identity.createIdentity`

## 0.1.9 -- 2025-07-08

* Option to show details or delete a conversation by giving index parameter without first selecting it
* Improved handling of ICE connections
* Automatic discovery of peers for pending direct messages

## 0.1.8.1 -- 2025-03-29

* Fix build from sdist (add missing include)

## 0.1.8 -- 2025-03-28

* Discovery service without requiring ICE support
* Added `/delete` command to delete chatrooms for current user
* Ignore record items with unexpected type
* Support GHC 9.12

## 0.1.7 -- 2024-10-30

* Chatroom-specific identity
* Secure cookie for connection initialization
* Support multiple public peers
* Handle unknown object and record item types
* Keep unknown items in local state

## 0.1.6 -- 2024-08-12

* Chatroom members list and join/leave commands
* Fix sending multiple data responses in a stream
* Added `--storage`/`--memory-storage` command-line options
* Compatibility with GHC up to 9.10
* Local discovery with IPv6

## 0.1.5 -- 2024-07-16

* Public chatrooms for multiple participants
* Send keep-alive packets on idle connection
* Windows support

## 0.1.4 -- 2024-06-11

* Added `/conversations` command to list and select conversations
* Added `/details` command for info about selected conversation
* Handle peer reconnection after its restart
* Support non-interactive mode without tty

## 0.1.3 -- 2024-05-05

* Enable/disable network services by command-line parameters
* Tab-completion of command name
* Implemented streams in network protocol
* Compatibility with GHC up to 9.8

## 0.1.2 -- 2024-02-20

* Compatibility with GHC up to 9.6
* Pruned unnecessary dependencies and fixed bounds

## 0.1.1 -- 2024-02-18

* Added build flag to enable/disable ICE support with pjproject.
* Added `-V` command-line switch to show version.

## 0.1.0 -- 2024-02-10

* First version.

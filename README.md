Erebos
======

The erebos binary provides simple CLI interface to the decentralized Erebos
messaging service. Local identity is created on the first run. Protocol and
services specification is being written at:

[https://erebosprotocol.net/spec](https://erebosprotocol.net/spec)

Erebos identity is based on locally stored cryptographic keys, all
communication is end-to-end encrypted. Multiple devices can be attached to the
same identity, after which they function interchangeably, without any one being
in any way "primary"; messages and other state data are then synchronized
automatically whenever the devices are able to connect with one another.

Status
------

This is experimental implementation of yet unfinished specification, so
changes, especially in the library API, are expected. Storage format and
network protocol should generally remain backward compatible, with their
respective versions to be increased in case of incompatible changes, to allow
for interoperability even in that case.

Usage
-----

On the first run, local identity will be created for this device based on
interactive prompts for:

`Name:` name of the user/owner, which will be shared among all devices
belonging to the same user; keep empty when initializing device that is going
to be attached to already existing identity on other device.

`Device:` name describing current device, can be empty.

After the initial setup, the erebos tool presents interactive prompt for
messages and commands. All commands start with the slash (`/`) character,
followed by command name and parameters (if any) separated by spaces. When
a conversation is selected, message to send there is entered directly on
the command prompt.

The session can be terminated either by end-of-input (typically `Ctrl-d`) or
using the `/quit` command.

### Example

Start `erebos` CLI and create new identity:
```
Name: Some Name
Device: First device
Some Name / First device
> 
```

Add public peer:
```
> /peer-add-public
[1] PEER NEW <unnamed> [37.221.243.57 29665]
[1] PEER UPD discovery1.erebosprotocol.net [37.221.243.57 29665]
```

Select the peer and send it a message, the public server just responds with
automatic echo message:
```
> /1
discovery1.erebosprotocol.net> hello
[18:55] Some Name: hello
[18:55] discovery1.erebosprotocol.net: Echo: hello
```

List chatrooms known to the peers:
```
> /chatrooms
[1] Test chatroom
[2] Second test chatroom
```

Enter a chatroom and send a message there:
```
> /1
Test chatroom> Hi
Test chatroom [19:03] Some Name: Hi
```

### Messaging

`/peers`  
: List peers with direct network connection. Peers are discovered automatically
  on local network or can be manually added.

`/contacts`  
: List known contacts (see below).

`/conversations`  
: List started conversations with contacts or other peers.

`/<number>`  
: Select conversation, contact or peer `<number>` based on the last
  `/conversations`, `/contacts` or `/peers` output list.

`<message>`  
: Send `<message>` to selected conversation.

`/history`  
: Show message history of the selected conversation.

`/details`  
: Show information about the selected conversations, contact or peer.

### Chatrooms

Currently only public unmoderated chatrooms are supported, which means that any
network peer is allowed to read and post to the chatroom. Individual messages
are signed, so message author can not be forged.

`/chatrooms`  
: List known chatrooms.

`/chatroom-create-public [<name>]`  
: Create public unmoderated chatroom. Room name can be passed as command
  argument or entered interactively.

`/members`  
: List members of the chatroom – usesers who sent any message or joined via the
`join` command.

`/join`  
: Join chatroom without sending text message.

`/join-as <name>`  
: Join chatroom using a new identity with a name `<name>`. This new identity is
  unrelated to the main one, and will be used for any future messages sent to
  this chatroom.

`/leave`  
: Leave the chatroom. User will no longer be listed as a member and erebos tool
  will no longer collect message of this chatroom.

### Add contacts

To ensure the identity of the contact and prevent man-in-the-middle attack,
generated verification code needs to be confirmed on both devices to add
contacts to contact list (similar to bluetooth device pairing). Before adding
new contact, list peers using `/peers` command and select one with `/<number>`.

`/contacts`  
: List already added contacts.

`/contact-add`  
: Add selected peer as contact. Six-digit verification code will be computed
  based on peer keys, which will be displayed on both devices and needs to be
  checked that both numbers are same. After that it needs to be confirmed using
  `/contact-accept` to finish the process.

`/contact-accept`  
: Confirm that displayed verification codes are same on both devices and add
  the selected peer as contact. The side, which did not initiate the contact
  adding process, needs to select the corresponding peer with `/<number>`
  command first.

`/contact-reject`  
: Reject contact request or verification code of selected peer.

### Attach other devices

Multiple devices can be attached to single identity to be used by the same
user. After the attachment process completes the roles of the devices are
equivalent, both can send and receive messages independently and those
messages, along with any other sate data, are synchronized automatically
whenever the devices can connect to each other.

The attachment process and underlying protocol is very similar to the contact
adding described above, so also generates verification code based on peer keys
that needs to be checked and confirmed on both devices to avoid potential
man-in-the-middle attack.

Before attaching device, list peers using `/peers` command and select the
target device with `/<number>`.

`/attach`  
: Attach current device to the selected peer. After the process completes the
  owner of the selected peer will become owner of this device as well.
  Six-digit verification code will be displayed on both devices and the user
  needs to check that both are the same before confirmation using the
  `/attach-accept` command.

`/attach-accept`  
: Confirm that displayed verification codes are same on both devices and
  complete the attachment process (or wait for the confirmation on the peer
  device). The side, which did not initiate the attachment process, needs to
  select the corresponding peer with `/<number>` command first.

`/attach-reject`  
: Reject device attachment request or verification code of selected peer.

### Other

`/peer-add <host> [<port>]`  
: Manually add network peer with given hostname or IP address.

`/peer-add-public`  
: Add known public network peer(s).

`/peer-drop`  
: Drop the currently selected peer. Afterwards, the connection can be
  re-established by either side.

`/update-identity`  
: Interactively update current identity information

`/quit`  
: Quit the erebos tool.


Storage
-------

Data are by default stored under `XDG_DATA_HOME`, typically
`$HOME/.local/share/erebos`, unless there is an erebos storage already
in `.erebos` subdirectory of the current working directory, in which case the
latter one in used instead. This can be overriden by `EREBOS_DIR` environment
variable.

Private keys are currently stored in plaintext under the `keys` subdirectory of
the erebos directory.

Erebos
======

The erebos binary provides simple CLI interface to the decentralized Erebos
messaging service. Local identity is created on the first run. Protocol and
services specification is being written at:

[http://erebosprotocol.net](http://erebosprotocol.net)

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
followed by command name and parameters (if any) separated by spaces. When a
peer or contact is selected, message to send him can be entered directly on the
command prompt.

### Messaging

`/peers`  
List peers with direct network connection. Peers are discovered automatically
on local network or can be manually added.

`/contacts`  
List known contacts (see below).

`/<number>`  
Select contact or peer `<number>` based on previous `/contacts` or `/peers`
output list.

`<message>`  
Send `<message>` to selected contact.

`/history`  
Show message history for selected contact or peer.

### Add contacts

To ensure the identity of the contact and prevent man-in-the-middle attack,
generated verification code needs to be confirmed on both devices to add
contacts to contact list (similar to bluetooth device pairing). Before adding
new contact, list peers using `/peers` command and select one with `/<number>`.

`/contacts`  
List already added contacts.

`/contact-add`  
Add selected peer as contact. Six-digit verification code will be computed
based on peer keys, which will be displayed on both devices and needs to be
checked that both numbers are same. After that it needs to be confirmed using
`/contact-accept` to finish the process.

`/contact-accept`  
Confirm that displayed verification codes are same on both devices and add the
selected peer as contact. The side, which did not initiate the contact adding
process, needs to select the corresponding peer with `/<number>` command first.

`/contact-reject`  
Reject contact request or verification code of selected peer.

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
Attach current device to the selected peer. After the process completes the
owner of the selected peer will become owner of this device as well. Six-digit
verification code will be displayed on both devices and the user needs to check
that both are the same before confirmation using the `/attach-accept` command.

`/attach-accept`  
Confirm that displayed verification codes are same on both devices and complete
the attachment process (or wait for the confirmation on the peer device). The
side, which did not initiate the attachment process, needs to select the
corresponding peer with `/<number>` command first.

`/attach-reject`  
Reject device attachment request or verification code of selected peer.

### Other

`/peer-add <host> [<port>]`  
Manually add network peer with given hostname or IP address.

`/update-identity`  
Interactively update current identity information


Storage
-------

Data are by default stored within `.erebos` subdirectory of the current working
directory. This can be overriden by `EREBOS_DIR` environment variable.

Private keys are currently stored in plaintext under the `keys` subdirectory of
the erebos directory.

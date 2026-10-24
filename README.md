<picture>
  <source media="(prefers-color-scheme: dark)" srcset=".github/logo-dark.png">
  <img src=".github/logo-light.png" alt="OpenKogama Photon" width="400">
</picture>

Photon UDP transport for KoGaMa clients. Takes datagrams, gives operations and events.

Handles the handshake, acknowledgements, sequence numbers, resends, fragmentation
and the Protocol16 encoding. Game logic lives somewhere else.

## Use

```csharp
var server = new PhotonServer(5055) { Log = Console.WriteLine };

server.Connected += peer => Console.WriteLine($"{peer} joined");
server.Disconnected += (peer, reason) => Console.WriteLine($"{peer} left, {reason}");
server.Operation += (peer, request) =>
{
    var response = new OperationResponse(request);
    response[1] = "hello";
    peer.Send(response);
};

await server.RunAsync(token);
```

`peer.Send(EventData, channel, reliable)` pushes an event without being asked,
`peer.Tag` holds whatever the game logic wants to keep per player and
`peer.Disconnect()` drops the connection.

Callbacks run on the receive loop, one at a time.

The client opens channels 0 and 1, plus 255 for control commands. Anything else
it drops.

Big payloads are split into fragments and sent in one burst, without pacing. Fine
on a local network, on a bad line the resends will take over.

## Credits

Made by nightus.

- **Becko**: huge thanks, the main inspiration for this project, and the packet captures
- **LazyLemon**: info on how things looked in old versions, help with the launcher, founding the [r/OpenKogama](https://www.reddit.com/r/OpenKogama/) subreddit, and translating the website into Russian
- **bustersky**: archiving the standalone builds
- **kamilslimak**: finding the 2012 build
- **Dylinn**: announcing OpenKogama on Regama
- **Zpayer**: announcing OpenKogama
- **loafio**: sharing OpenKogama on his server
- **Limeen**: translating the website into French
- **lordscorpioni**: translating the website into Spanish

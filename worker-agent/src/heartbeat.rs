pub async fn send_heartbeats() {

    //needs to open the bidirectional stream for heartbeat RPC
    //fire a HeartbeatPing every 5 seconds
    //have broken stream reconnect after a certain timeout
    //when StopAcceptingWork is received from server, worker is dead, have flag set when it happens?
    //self kill after?

    tokio::spawn(async move {
        loop {

        }
    });
}
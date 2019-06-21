# firmament-go
kubernetes, scheduling, batching scheduling, network flow

TODOs:
1. integrate solver code into flow scheduler;
2. revisit all TODOs commented among coding files;
3. we need to revisit all the panic logic (log.Panic(...));
4. add mutex for scheduler functions, just like Firmament does;
5. add bool args to control the logic to handle migration and eviction conditions;

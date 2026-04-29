import os
from spacetime import Node
from utils.pcc_models import Register

def init(df, user_agent, fresh):
    reg = df.read_one(Register, user_agent)
    if not reg:
        reg = Register(user_agent, fresh)
        df.add_one(Register, reg)
        df.commit()
        df.push_await()
    print("Waiting for cache server assignment...", flush=True)
    while not reg.load_balancer:
        df.pull_await()
        if reg.invalid:
            raise RuntimeError("User agent string is not acceptable.")
        if reg.load_balancer:
            df.delete_one(Register, reg)
            df.commit()
            df.push()
    return reg.load_balancer

def get_cache_server(config, restart):
    print(
        f"Registering with cache coordinator {config.host}:{config.port}...",
        flush=True)
    init_node = Node(
        init, Types=[Register], dataframe=(config.host, config.port))
    cache_server = init_node.start(
        config.user_agent, restart or not os.path.exists(config.save_file))
    print(f"Using cache server {cache_server[0]}:{cache_server[1]}.", flush=True)
    return cache_server

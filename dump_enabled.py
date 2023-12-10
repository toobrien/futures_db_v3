from contract_settings  import CONTRACT_SETTINGS 
from json               import dumps
from sys                import argv


if __name__ == "__main__":

    dump_mode   = argv[1]
    sym_mode    = argv[2]
    enabled     = []

    for sym, dfn in CONTRACT_SETTINGS.items():

        if "enabled" in dfn and dfn["enabled"]:

            if "globex" in dfn:

                enabled.append(f"{dfn['globex']}.{sym_mode}")

    if dump_mode == "json":

        print(dumps(enabled, indent = 2))

    elif dump_mode == "cmd":

        print(" ".join(enabled))

from contract_settings  import CONTRACT_SETTINGS 
from json               import dumps
from sys                import argv


if __name__ == "__main__":

    mode        = arv[1]
    enabled     = []

    for sym, dfn in CONTRACT_SETTINGS.items():

        if "enabled" in dfn and dfn["enabled"]:

            if "globex" in dfn:

                enabled.append(f"{dfn['globex']}.FUT")

    if mode == "json":

        print(dumps(enabled, indent = 2))

    elif mode == "cmd":

        print(" ".join(enabled)

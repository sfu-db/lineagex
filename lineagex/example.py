import os

from .lineagex import lineagex


class example:
    def __init__(self, case: str = ""):
        def get_current_path():
            # Returns the path of the current script
            return os.path.dirname(os.path.realpath(__file__))

        cwd = get_current_path()
        if case == "github_example":
            t_noconn = lineagex(
                sql=os.path.join(cwd, "examples", "github_example"),
                target_schema="schema1",
                search_path_schema="schema1, public",
            )
            print(
                "{} finished, please check in the folder for output.json and index.html".format(
                    case
                )
            )
        elif case == "dependency_example":
            t_noconn = lineagex(
                sql=os.path.join(cwd, "examples", "dependency_example"),
                target_schema="mimiciii_derived",
                search_path_schema="mimiciii_clinical, public",
            )
            print(
                "{} finished, please check in the folder for output.json and index.html".format(
                    case
                )
            )
        elif case == "mimic-iv":
            t_noconn = lineagex(
                sql=os.path.join(cwd, "examples", "mimic-iv"),
                target_schema="mimiciv_derived",
                search_path_schema="mimiciv_icu, mimiciv_hosp",
            )
            print(
                "{} finished, please check in the folder for output.json and index.html".format(
                    case
                )
            )
        elif case == "mimic-iii":
            t_noconn = lineagex(
                sql=os.path.join(cwd, "examples", "mimic-iii"),
                target_schema="mimiciv_derived",
                search_path_schema="mimiciii_clinical, public",
            )
            print(
                "{} finished, please check in the folder for output.json and index.html".format(
                    case
                )
            )


if __name__ == "__main__":
    example("mimic-iii")

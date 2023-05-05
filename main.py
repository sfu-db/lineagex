from lineage import Lineage
import os

if __name__ == '__main__':
    lineage_output = Lineage(os.path.dirname(os.path.dirname(os.path.dirname(os.getcwd()))))

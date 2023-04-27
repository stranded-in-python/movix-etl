from watchfiles import run_process

from load_data import transfer

if __name__ == '__main__':
    run_process('/app', target=transfer, debug=True)

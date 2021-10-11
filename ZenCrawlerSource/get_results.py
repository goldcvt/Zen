import argparse
import psycopg2
import sys


# TODO finish arument section
# argument section

def main():
    arg_parser = argparse.ArgumentParser(description='Display results of y.zen crawls')

    arg_parser.add_argument('--limit', type=int, default=20, help='Limits the resulting output string number')
    arg_parser.add_argument('--sort', type=str, default='', help='A column to sort by')

    args = arg_parser.parse_args()

    for i in range(args.limit):
        sys.stdout.write(str(run_query(args)))
        sys.stdout.write('\n')

# sql connection section
def establish_conn():
    return psycopg2.connect("zen_copy", "postgres", "", "127.0.0.1")


# sql queries and logic
def run_query(args):
    pass
    # MAKE AN OUTPUT LINE


if __name__ == '__main__':
    main()

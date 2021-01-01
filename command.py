import argparse
import logging
import sys
from .outputs import LoggingConfig, PackageLogFilter
from .oauth import OAuth2, GoogleOAuth
from .outputs import Print
logger = logging.getLogger(__name__)


class Program:
    """Contains static methods for sub-commands to start the processing program with args

    The name of the methods are the same as the name of the sub parsers in subparsers.add_parser()
    """
    @staticmethod
    def oauth(args):
        auth = OAuth2(
            client_id=args.id, 
            client_secret=args.secret, 
            auth_endpoint=args.auth_url, 
            token_endpoint=args.token_url
        )
        if args.redirect:
            auth_url = auth.authentication_url(scope=args.scope, redirect_uri=args.redirect)
        else:
            auth_url = auth.authentication_url(scope=args.scope)
        Print.blue("Please visit the following URL to authenticate:")
        print(auth_url)
        print()
        code = input("Enter the code you get after authentication:")
        if args.redirect:
            response = auth.exchange_token(code, redirect_uri=args.redirect)
        else:
            response = auth.exchange_token(code)
        Print.green("OAuth Tokens:")
        Print.print(response)

    @staticmethod
    def auth_google(args):
        """Authenticate with Google APIs
        """
        auth = GoogleOAuth(client_id=args.id, client_secret=args.secret)
        auth_url = auth.authentication_url(scope=args.scope)
        Print.blue("Please visit the following URL to authenticate with Google:")
        print(auth_url)
        print()
        code = input("Enter the code you get after authentication:")
        response = auth.exchange_token(code)
        Print.green("OAuth Tokens:")
        Print.print(response)


def main():
    parser = argparse.ArgumentParser(description="Entry points to Aries command line tools.")
    subparsers = parser.add_subparsers(title="Program", help="Program", dest='program')

    sub_parser = subparsers.add_parser(
        "oauth",
        help="Authenticate with OAuth 2.0 to obtain access token and refresh token."
    )
    sub_parser.add_argument('--auth_url', type=str, required=True, help="OAuth authentication endpoint")
    sub_parser.add_argument('--token_url', type=str, required=True, help="OAuth token endpoint")
    sub_parser.add_argument('--id', type=str, required=True, help="OAuth client ID")
    sub_parser.add_argument('--secret', type=str, required=True, help="OAuth client secret")
    sub_parser.add_argument('--scope', type=str, required=True, nargs='+', help="OAuth Scopes")
    sub_parser.add_argument('--redirect', type=str, help="Redirect URL")

    sub_parser = subparsers.add_parser(
        "auth_google",
        help="Authenticate with Google to obtain access token and refresh token."
    )
    sub_parser.add_argument('--id', type=str, required=True, help="Google OAuth client ID")
    sub_parser.add_argument('--secret', type=str, required=True, help="Google OAuth client secret")
    sub_parser.add_argument('--scope', type=str, required=True, nargs='+', help="OAuth 2.0 Scopes for Google APIs")

    # Parse command
    args = parser.parse_args()
    # Show help if no sub parser matched.
    if not vars(args).keys() or not args.program or not hasattr(Program, args.program):
        parser.parse_args(["-h"])
        return

    # Run program
    func = getattr(Program, args.program)
    func(args)
    sys.exit(0)


if __name__ == '__main__':
    with LoggingConfig(filters=[PackageLogFilter(packages="Aries")]):
        main()

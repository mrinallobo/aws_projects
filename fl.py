import tweepy
import time

# Replace these with your own API keys and access tokens
api_key = "iwp2xEOKk9AdgmBDXzVLVn24L"
api_secret = "VpTOJjlHkuyalpcmS40w3OQYriALybH3crzk7IkZFR70DHyRA8"
bearer_token = "AAAAAAAAAAAAAAAAAAAAAM9VrwEAAAAAdjGQHUIbFw1vAQLulxTdcn2zuRQ%3DRh5HFtnMOlCjyFYKqtAjCEdLv45o5LlWCZwLaryVzc2l5u7Ikx"
access_token = "1743537760428781569-I7jshYQ0UXXlj6lRmwXjCGX1hJlwAY"
access_token_secret = "DrDbKYc6pAezfscNCe5ijdPO1apFfc7kwWHgxtTXrdmHo"

# Initialize Tweepy client and authentication
client = tweepy.Client(bearer_token, api_key, api_secret, access_token, access_token_secret)
auth = tweepy.OAuth1UserHandler(api_key, api_secret, access_token, access_token_secret)
api = tweepy.API(auth)

# List of usernames to check
usernames = ["UsdGod"]

# Function to get the user's account link


# Function to get the user's account link
def get_account_link(username):
    user = api.get_user(screen_name=username)
    return f"https://twitter.com/{user.screen_name}"

# Keep checking every 5 minutes
while True:
    for username in usernames:
        try:
            # Get the user's following list
            following = api.get_users_following(screen_name=username, count=5)

            # Check if any of the followed users were followed in the last 5 minutes
            for user in following:
                if (time.time() - user.created_at.timestamp()) / 60 < 5:
                    new_account_username = user.screen_name
                    print(f"{username} followed a new account: {new_account_username}")
                    print(f"Account link: {get_account_link(username)}")
                    break

        except tweepy.error.TweepError as e:
            print(f"Error: {e}")

    # Wait for 5 minutes
    time.sleep(300)
"""TODO"""

import os
import logging
import pandas as pd
import tweepy
import yaml


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def load_data_sources(config: dict):
    """Loads all necessary data files for the bot using paths from the config."""
    # Build absolute paths from the project root for robustness
    # CORRECTED: Go up three levels ('..', '..', '..') to reach the project root from src/bots/x
    project_root = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..")
    )

    patterns_path = os.path.join(project_root, config["data_paths"]["top_patterns"])
    countries_path = os.path.join(project_root, config["data_paths"]["countries"])

    try:
        logging.info(f"Loading top patterns from: {patterns_path}")
        patterns_df = pd.read_parquet(patterns_path)

        logging.info(f"Loading countries from: {countries_path}")
        countries_df = pd.read_csv(countries_path)

        country_map = dict(zip(countries_df["id"], countries_df["name_1"]))
        return patterns_df, country_map
    except FileNotFoundError as e:
        logging.error(
            f"Could not load data file: {e}. Make sure paths in config_tokens.yaml are correct relative to the project root."
        )
        return None, None


class QGIXBot:
    """A bot for posting Quantitative Geopolitical Intelligence patterns to X."""

    def __init__(self, tokens: dict, patterns_df: pd.DataFrame, country_map: dict):
        """Initializes the bot with API tokens and data."""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.patterns_df = patterns_df
        self.country_map = country_map

        # Authenticate using Tweepy's Client for v2 API access
        self.client = tweepy.Client(
            bearer_token=tokens["Authentication Tokens"]["Bearer Token"],
            consumer_key=tokens["API Key"],
            consumer_secret=tokens["API Key Secret"],
            access_token=tokens["Authentication Tokens"]["Access Token"],
            access_token_secret=tokens["Authentication Tokens"]["Access Token Secret"],
        )
        self.logger.info("Bot initialized and authenticated with X API.")

    def _select_pattern(self) -> pd.Series:
        """Selects a random pattern from the top 100 for variety."""
        top_slice = self.patterns_df.head(100)
        selected = top_slice.sample(n=1).iloc[0]
        self.logger.info(f"Selected pattern: {dict(selected)}")
        return selected

    def _format_tweet(self, pattern: pd.Series) -> str:
        """Formats the selected pattern data into a tweet string."""
        country_a_name = self.country_map.get(pattern["country_a_id"], "Unknown")
        country_b_name = self.country_map.get(pattern["country_b_id"], "Unknown")

        score_100 = pattern["SignificanceScore"] * 100

        tweet_text = (
            f"⚡ QGI Pattern Detected ⚡\n\n"
            f"Countries: {country_a_name} & {country_b_name}\n"
            f"Timeframe: {int(pattern['start_year_a'])}–{int(pattern['start_year_a'] + pattern['length'])}\n"
            f"Indicators: {int(pattern['scdi_count'])}\n"
            f"Correlation: {pattern['average_correlation']:.2f}\n\n"
            f"Significance Score: {score_100:.0f}/100\n\n"
            f"#Geopolitics #DataScience #QGI #Economics"
        )
        self.logger.info(f"Formatted tweet:\n{tweet_text}")
        return tweet_text

    def post(self, tweet_text: str):
        """Posts the formatted text to X."""
        try:
            response = self.client.create_tweet(text=tweet_text)
            self.logger.info(
                f"Tweet posted successfully! Tweet ID: {response.data['id']}"
            )
            self.logger.info(
                f"View on X: https://x.com/user/status/{response.data['id']}"
            )
        except tweepy.errors.TweepyException as e:
            self.logger.error(f"Failed to post tweet: {e}")

    def run(self):
        """Executes the full workflow to post one tweet."""
        self.logger.info("Running bot workflow...")
        pattern = self._select_pattern()
        tweet = self._format_tweet(pattern)
        self.post(tweet)


if __name__ == "__main__":
    # Load configuration from the YAML file
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_dir, "config_tokens.yaml")
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        logging.critical(
            "FATAL: config_tokens.yaml not found. Please create it next to this script."
        )
        exit()

    # Load data using paths from the config
    patterns_data, countries = load_data_sources(config)

    # Run the bot if data loaded successfully
    if patterns_data is not None and countries is not None:
        qgi_bot = QGIXBot(
            tokens=config["tokens"], patterns_df=patterns_data, country_map=countries
        )
        qgi_bot.run()
    else:
        logging.error("Bot could not run due to data loading failure.")

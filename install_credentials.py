import os
import json


if __name__ == "__main__":
    # Load credentials from JSON file
    try:
        with open("setup_credentials.json") as f:
            credentials = json.load(f)

        # Add credentials to environment variables
        for key, value in credentials.items():
            os.environ[key] = value
    except FileNotFoundError:
        print(
            """Please add credentials for Datrabricks 
            SQL warehouse in a file called 
            "setup_credentials.json" to the root 
            directory of this project. 
            Then run install_credentials.py."""
        )

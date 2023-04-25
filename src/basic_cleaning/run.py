#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):
    '''
    Run the data cleaning step
    '''

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    
    logger.info("download artifact: %s", args.input_artifact)
    df = pd.read_csv(artifact_local_path)
    ######################
    # YOUR CODE HERE     #
    ######################
    logger.info("Removing outliers, with price threshold [min-max]: %i - %i", args.min_price, args.max_price)
    min_price = args.min_price
    max_price = args.max_price
    idx = df["price"].between(min_price, max_price)
    df = df[idx].copy()
    
    # Convert last_review to datetime
    df['last_review'] = pd.to_datetime(df['last_review'])

    # Save the results to a CSV file
    logger.info("Save the clean data to CSV")
    df.to_csv(args.output_artifact, index=False)

    # Build artifact
    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )

    logger.info("Log artifact: clean_sample.csv")
    artifact.add_file(args.output_artifact)
    run.log_artifact(artifact)
    


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help='Input artifact name',
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,## INSERT TYPE HERE: str, float or int,
        help="Output artifact name",## INSERT DESCRIPTION HERE,
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,## INSERT TYPE HERE: str, float or int,
        help="Artifact Output type",## INSERT DESCRIPTION HERE,
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,## INSERT TYPE HERE: str, float or int,
        help="Description for the output",## INSERT DESCRIPTION HERE,
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=int,## INSERT TYPE HERE: str, float or int,
        help="Min price for outlier threshold",## INSERT DESCRIPTION HERE,
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=int,## INSERT TYPE HERE: str, float or int,
        help="Max price for outlier threshold",## INSERT DESCRIPTION HERE,
        required=True
    )


    args = parser.parse_args()

    go(args)

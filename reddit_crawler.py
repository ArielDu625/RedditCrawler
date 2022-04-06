import requests
from datetime import datetime
import time
import pandas as pd
import praw

# fill in the authentication information
reddit = praw.Reddit(user_agent='',
                     client_id='',
                     client_secret='',
                     username='',
                     password='')

submission_url = "https://api.pushshift.io/reddit/search/submission"
submission_fields = ['author', 'author_flair_text', 'author_fullname', 'created_utc', 'id', 'link_flair_text',
                     'num_comments', 'selftext', 'title', 'full_link']


def crawl_comments(submission_urls):
    comments = []
    for submission_url in submission_urls:
        print(f"processing {submission_url}")
        try:
            submission = reddit.submission(url=submission_url)
            submission.comments.replace_more(limit=None)
        except:
            continue

        else:
            for comment in submission.comments.list():
                try:
                    author_fullname = comment.author.fullname
                    print(f"post:{comment.link_id}, comment:{comment.id}, author_fullname={author_fullname}")
                except:
                    continue
                else:
                    cmt_data = {"link_id": comment.link_id,
                                "author": str(comment.author),
                                "author_fullname": comment.author.fullname,
                                "author_flair_text": comment.author_flair_text,
                                "id": comment.id,
                                "body": comment.body,
                                "score": comment.score,
                                "comment_karma": comment.author.comment_karma,
                                "link_karma": comment.author.link_karma,
                                "parent_id": comment.parent_id,
                                "is_submitter": comment.is_submitter
                                }
                    comments.append(cmt_data)
    return comments


def crawl_submission_page(subreddit: str, last_page_time=None):
    """
    :param subreddit: str
    :param last_page_time: int
    :return:
    """
    params = {"subreddit": subreddit,
              "size": 500,
              "sort": "desc",
              "sort_type": "created_utc",
              "fields": submission_fields}
    if last_page_time is not None:
        params["before"] = last_page_time

    results = requests.get(submission_url, params)
    if not results.ok:
        raise Exception("Server returned status code {}".format(results.status_code))
    return results.json()['data']


def crawl_subreddit(subreddit: str, start_time: datetime):
    """

    :param subreddit:
    :param start_time:
    :return:
    """
    submissions = []
    last_page_time = None
    while (last_page_time is None) or (datetime.utcfromtimestamp(last_page_time) > start_time):
        pages = crawl_submission_page(subreddit, last_page_time)
        submissions += pages

        last_page_time = pages[-1]['created_utc']
        print(f"subreddit={subreddit}, start_time={datetime.utcfromtimestamp(last_page_time)}")
        time.sleep(1)

    return submissions


if __name__ == "__main__":
    subreddits = ['singlemoms', 'transgender', 'uberdrivers']
    start_times = ["2020-08-01", "2020-08-01", "2021-10-01"]

    collect_comments = True

    for subreddit, start_time in zip(subreddits, start_times):
        start_date = datetime.strptime(start_time, '%Y-%m-%d')
        submissions = crawl_subreddit(subreddit, start_date)
        df = pd.DataFrame(submissions)

        filename = "./reddit_data/" + subreddit + "_" + start_time + "_post.csv"
        df.to_csv(filename)

        if collect_comments:
            submission_urls = df['full_link'].tolist()
            comments = crawl_comments(submission_urls)
            cdf = pd.DataFrame(comments)
            cfilename = './reddit_data/' + subreddit + "_" + start_time + "_comments.csv"
            cdf.to_csv(cfilename)


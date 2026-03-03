from flask import Flask, jsonify
from dummy_tests.get_dummy_candidate import generate_dummy_candidate
from dummy_tests.get_dummy_jobs import generate_dummy_jobs

app = Flask(__name__)

@app.route("/kaneru_job/get_user_messages", methods=["POST"])
def get_user_messages():
    candidates = [generate_dummy_candidate(i) for i in range(1, 13)]
    print(candidates)
    return jsonify({
        "deck_version": "v1",
        "count": len(candidates),
        "candidates": candidates,
    })
    
@app.route("/kaneru_job/get_job_postings", methods=["POST"])
def get_job_postings():
    jobs = [generate_dummy_jobs(i) for i in range(1, 11)]
    print(jobs)
    return jsonify({
        "deck_version": "v1",
        "count": len(jobs),
        "postings": jobs,
    })


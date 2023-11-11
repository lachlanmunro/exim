# exim

It's been useful to me several times and the format doesn't change. Maybe it's useful to you too. But it's getting no work, its archived, just take what you want.

##

A so hacky that it should've been a bash script golang program for parsing exim4 logs to pull a csv of emails against their "owners".

## Purpose

Reads exim4 email server logs to create a list of email addresses sent to (regex match `-ignore "^$"` so you can discard internal mail/spam) grouped by email sent from (regex match `-email ".*"` so you can ignore no-reply etc). Results in a file with email addresses sent to by local addresses. Outbound emails only. I've had it churn through a five or so years worth of email logs without too much trouble.

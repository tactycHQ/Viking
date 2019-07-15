import twint

# Configure
c = twint.Config()
# c.Username = "noneprivacy"
c.Search = '"The Bride Test" OR "the bride test" AND Helen Hoang AND (read OR reads OR reading OR book OR books OR novel OR author) -filter:retweets -filter:links -filter:replies since:2019-07-01 until:2019-07-05'
c.Format = "Tweet id: {id} | Tweet: {tweet} | Date:{date}"

# Run
twint.run.Search(c)
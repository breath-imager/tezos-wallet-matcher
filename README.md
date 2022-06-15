A smol 🛠️ which will retrieve a particular contract on Objkt and collects the active offers and stores them in a Pandas dataframe. This will then be compared with the list of Tezos wallets of all holders of Season 0 Pass which is being stored in a PostgreSQL database running on Heroku.

- Query Objkt GraphQL API for a particular NFT's active offer list. Store results in Pandas dataframe.
- Retrieve tezos wallet list of season pass holders from postgres on heroku and store results in dataframe.
- Compare the two dataframes to find wallets common to both lists. Return result as JSON.

### Reddit Growth Tracker

##### Author: Anjala
##### Version: V1


The goal of this data engineering project is to analyze and visualize the trends on Reddit for a specific period, focusing on the most popular subreddits and topics. By doing so, we aim to gain insights into the dynamic nature of trending content, and gather insights about the performance of the app

#### Project Summary:

1. Data Extraction : Get data on the subreddits; for simplicity of the project, get data on top 25 subreddits. This can be used as a proxy for the subreddit universe.
2. Data Modelling, Loading : Post/Comment data is loaded in Mongo DB and then deduplicated and moved to postgres. Final table is the dim_all_users table
3. Implement KPI: 
   a. Weekly Daily Users
4. Implement a Realtime pipeline: This is a simple streaming service that notes if a nsfw post is made in a non nsfw thread. 
5. Visualization
6. Testing

Architecture FlowChart

![img_1.png](img_1.png)!



Work in progress 

An algorithm predicting the conditions of ice and mountain climbing routes: will this icefall that I want to climb be frozen and solid enough tomorrow or next week?

Brief description of the process:

V0:
1 - Scraping data from camptocamp.org + set up and SQL db (training data with historical ice fall and mountain routes conditions + information about the route like altitude, orientation..) ✅ 

2 - Baseling algorithm: using only neighbouring and similar routes information (WIP)

3 - Build a RAG based AI agent so that will suggest climbable routes to the user that meet its difficulty and location requirements

V1:

1 - Scraping open source historical weather data from meteofrance.com 

2 - Enhance the model by integrating historical and forecast weahter data
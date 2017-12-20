# JMAP
Application layer mapping for storing and retrieving JSON object in a relational database 

**Setup Database**
Edit config.cfg to add Database name, and user credentials.

JMapper will take care of creating tables, and rest of the magic for storing JSON.

**Setup JMAPPER**
1, Install all the required libraries.
```
pip install -r requirements.txt
```

**Run JMapper**
```
python json_mapper.py
```

**Test case**
For the purpose of test case, JMapper uses all the JSON dataset located in ./JsonData folder. You can also add your own dataset there. By default, JMapper test case spawns single process for all the test case.

```
python json_mapper.py -t NUMBER -p PROCESS_COUNT
```
If NUMBER = 1, all test cases, Insert, read and update
   NUMBER = 2, Insert of all JSON
   NUMBER = 3, Random read of JSON
   NUMBER = 4, Random update of JSON

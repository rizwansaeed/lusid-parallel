![LUSID_by_Finbourne](https://content.finbourne.com/LUSID_repo.png)

This repo contains an example to show how to call the LUSID API using multiple thread. Note, this example currently uses blocking I/O call i.e. does not use aysncio

# Installation

Install the dependencies from the `requirements.txt`
```
pip install -r requirements.txt --use-deprecated=legacy-resolver
```

Configure your API credentials following the steps [here](https://support.lusid.com/knowledgebase/article/KA-01663/).

# Running

Run the following command

```
python main_thread.py -a secrets.json -n 10 -s multiload -t -f data/returns.csv
```

A larger upload can be run using 
```
python main_thread.py -a secrets.json -n 10 -s multiload -t -f data/returns2.csv
```

For futher details on options run `python main_thread.py -h`

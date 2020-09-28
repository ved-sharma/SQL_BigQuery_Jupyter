# Running SQL queries on BigQuery data from a Jupyter notebook

First [Connect Jupyter notebook to BigQuery API](https://github.com/ved-sharma/BigQuery_Jupyter_notebook_setup), if you haven't already done that.

I took Kaggle course - [Intro to SQL](https://www.kaggle.com/learn/intro-to-sql). Following example is one of the exercises in that course.

[Stack Overflow](https://stackoverflow.com/) is a popular question and answer site for technical questions. Their data is publicly available through BigQuery API.

Here, we would like to identify the Stack Overflow users who have demonstrated expertise with a specific technology (e.g. bigquery) by answering related questions about it.

Let's start with importing bigquery, connecting to stackoverflow dataset and taking a look at all the availble data (tables).


```python
from google.cloud import bigquery

# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "stackoverflow" dataset
dataset_ref = client.dataset("stackoverflow", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# Get a list of available tables 
tables = list(client.list_tables(dataset))

list_of_tables =[table.table_id for table in tables]
print(list_of_tables)
```

    ['badges', 'comments', 'post_history', 'post_links', 'posts_answers', 'posts_moderator_nomination', 'posts_orphaned_tag_wiki', 'posts_privilege_wiki', 'posts_questions', 'posts_tag_wiki', 'posts_tag_wiki_excerpt', 'posts_wiki_placeholder', 'stackoverflow_posts', 'tags', 'users', 'votes']
    

Dataset contains many tables. We will be looking at only two of those - "posts_questions" and "posts_answers".

First, let's take a look at the "posts_questions" table


```python
# Construct a reference to this table
questions_table_ref = dataset_ref.table("posts_questions")

# API request - fetch the table
questions_table = client.get_table(questions_table_ref)

# Preview the first five lines of the "posts_questions" table
client.list_rows(questions_table, max_results=5).to_dataframe()
```




<div>
<table border="0" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>title</th>
      <th>body</th>
      <th>accepted_answer_id</th>
      <th>answer_count</th>
      <th>comment_count</th>
      <th>community_owned_date</th>
      <th>creation_date</th>
      <th>favorite_count</th>
      <th>last_activity_date</th>
      <th>last_edit_date</th>
      <th>last_editor_display_name</th>
      <th>last_editor_user_id</th>
      <th>owner_display_name</th>
      <th>owner_user_id</th>
      <th>parent_id</th>
      <th>post_type_id</th>
      <th>score</th>
      <th>tags</th>
      <th>view_count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>33020748</td>
      <td>How in grails send with etag 302 status and no...</td>
      <td>&lt;p&gt;I add plugin &lt;/p&gt;\n\n&lt;pre&gt;&lt;code&gt;compile ":c...</td>
      <td>33021299.0</td>
      <td>1</td>
      <td>0</td>
      <td>None</td>
      <td>2015-10-08 16:03:35.450000+00:00</td>
      <td>None</td>
      <td>2015-10-08 16:31:49.093000+00:00</td>
      <td>2015-10-08 16:09:22.350000+00:00</td>
      <td>None</td>
      <td>5053192.0</td>
      <td>None</td>
      <td>5053192</td>
      <td>None</td>
      <td>1</td>
      <td>0</td>
      <td>grails|etag</td>
      <td>256</td>
    </tr>
    <tr>
      <th>1</th>
      <td>33021593</td>
      <td>HackLang by Facebook is not strict</td>
      <td>&lt;p&gt;Good day,&lt;/p&gt;\n\n&lt;p&gt;I have problem. I want ...</td>
      <td>33026520.0</td>
      <td>1</td>
      <td>0</td>
      <td>None</td>
      <td>2015-10-08 16:47:04.940000+00:00</td>
      <td>None</td>
      <td>2015-10-08 21:50:58.280000+00:00</td>
      <td>2015-10-08 21:50:58.280000+00:00</td>
      <td>user3477804</td>
      <td>NaN</td>
      <td>None</td>
      <td>3249421</td>
      <td>None</td>
      <td>1</td>
      <td>2</td>
      <td>hhvm|hacklang</td>
      <td>256</td>
    </tr>
    <tr>
      <th>2</th>
      <td>33028924</td>
      <td>Cloud Dataproc error - "Failed to load" in GDC</td>
      <td>&lt;p&gt;I'm trying to create a Dataproc cluster via...</td>
      <td>33029452.0</td>
      <td>1</td>
      <td>0</td>
      <td>None</td>
      <td>2015-10-09 02:27:56.367000+00:00</td>
      <td>None</td>
      <td>2015-11-16 20:15:23.607000+00:00</td>
      <td>NaT</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>2877278</td>
      <td>None</td>
      <td>1</td>
      <td>3</td>
      <td>google-cloud-platform|google-cloud-dataproc</td>
      <td>256</td>
    </tr>
    <tr>
      <th>3</th>
      <td>33041068</td>
      <td>Google play displays my app as not compatible ...</td>
      <td>&lt;p&gt;I want my app to be available only on phone...</td>
      <td>NaN</td>
      <td>1</td>
      <td>0</td>
      <td>None</td>
      <td>2015-10-09 14:40:25.690000+00:00</td>
      <td>None</td>
      <td>2015-10-09 14:52:13.987000+00:00</td>
      <td>NaT</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>3318949</td>
      <td>None</td>
      <td>1</td>
      <td>0</td>
      <td>android|android-install-apk</td>
      <td>256</td>
    </tr>
    <tr>
      <th>4</th>
      <td>33044239</td>
      <td>Confused about readlink /usr/lib/libpcre.so</td>
      <td>&lt;p&gt;I am following &lt;/p&gt;\n\n&lt;pre&gt;&lt;code&gt;http://ww...</td>
      <td>NaN</td>
      <td>1</td>
      <td>0</td>
      <td>None</td>
      <td>2015-10-09 17:42:37.293000+00:00</td>
      <td>None</td>
      <td>2015-10-09 23:37:14.230000+00:00</td>
      <td>2015-10-09 17:49:09.120000+00:00</td>
      <td>None</td>
      <td>5214008.0</td>
      <td>None</td>
      <td>5214008</td>
      <td>None</td>
      <td>1</td>
      <td>0</td>
      <td>linux</td>
      <td>256</td>
    </tr>
  </tbody>
</table>
</div>



The "id" column here identifies the ID of each question asked.
The "tags" column here lists the topics/technologies each question is about (e.g. bigquery).

Now, we will take a look at the "posts_answers" table.


```python
# Construct a reference to this table
answers_table_ref = dataset_ref.table("posts_answers")

# API request - fetch the table
answers_table = client.get_table(answers_table_ref)

# Preview the first five lines of the "posts_answers" table
client.list_rows(answers_table, max_results=5).to_dataframe()
```




<div>
<table border="0" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>title</th>
      <th>body</th>
      <th>accepted_answer_id</th>
      <th>answer_count</th>
      <th>comment_count</th>
      <th>community_owned_date</th>
      <th>creation_date</th>
      <th>favorite_count</th>
      <th>last_activity_date</th>
      <th>last_edit_date</th>
      <th>last_editor_display_name</th>
      <th>last_editor_user_id</th>
      <th>owner_display_name</th>
      <th>owner_user_id</th>
      <th>parent_id</th>
      <th>post_type_id</th>
      <th>score</th>
      <th>tags</th>
      <th>view_count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>33109176</td>
      <td>None</td>
      <td>&lt;p&gt;Your square flying diagonal is because that...</td>
      <td>None</td>
      <td>None</td>
      <td>0</td>
      <td>None</td>
      <td>2015-10-13 17:40:54.690000+00:00</td>
      <td>None</td>
      <td>2015-10-13 17:53:03.513000+00:00</td>
      <td>2015-10-13 17:53:03.513000+00:00</td>
      <td>None</td>
      <td>1737627.0</td>
      <td>None</td>
      <td>1737627</td>
      <td>33107490</td>
      <td>2</td>
      <td>0</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1</th>
      <td>33109178</td>
      <td>None</td>
      <td>&lt;p&gt;how about this, using &lt;code&gt;strsplit&lt;/code&gt;...</td>
      <td>None</td>
      <td>None</td>
      <td>0</td>
      <td>None</td>
      <td>2015-10-13 17:41:00.420000+00:00</td>
      <td>None</td>
      <td>2015-10-13 19:39:58.627000+00:00</td>
      <td>2015-10-13 19:39:58.627000+00:00</td>
      <td>None</td>
      <td>635843.0</td>
      <td>None</td>
      <td>635843</td>
      <td>33104951</td>
      <td>2</td>
      <td>0</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>33109182</td>
      <td>None</td>
      <td>&lt;p&gt;You need to load AngularJs before Cordova.&lt;...</td>
      <td>None</td>
      <td>None</td>
      <td>0</td>
      <td>None</td>
      <td>2015-10-13 17:41:09.587000+00:00</td>
      <td>None</td>
      <td>2015-10-13 17:41:09.587000+00:00</td>
      <td>NaT</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>5273311</td>
      <td>32664840</td>
      <td>2</td>
      <td>0</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>33109200</td>
      <td>None</td>
      <td>&lt;p&gt;I'm not sure, but I think that you need to ...</td>
      <td>None</td>
      <td>None</td>
      <td>0</td>
      <td>None</td>
      <td>2015-10-13 17:42:21.293000+00:00</td>
      <td>None</td>
      <td>2015-10-13 18:55:00.113000+00:00</td>
      <td>2015-10-13 18:55:00.113000+00:00</td>
      <td>None</td>
      <td>371184.0</td>
      <td>None</td>
      <td>2501136</td>
      <td>33026509</td>
      <td>2</td>
      <td>0</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>4</th>
      <td>33109204</td>
      <td>None</td>
      <td>&lt;p&gt;I guess you're missing a &lt;code&gt;condition&lt;/c...</td>
      <td>None</td>
      <td>None</td>
      <td>0</td>
      <td>None</td>
      <td>2015-10-13 17:42:29.307000+00:00</td>
      <td>None</td>
      <td>2015-10-13 17:42:29.307000+00:00</td>
      <td>NaT</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>4366287</td>
      <td>33109071</td>
      <td>2</td>
      <td>0</td>
      <td>None</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
</div>



"posts_answers" table has a column called "parent_id" which identifies the ID of the question each answer is responding to. 

The "parent_id" column of "posts_answers" maps to the "id" column of "posts_questions" table. So, we will be joining them.

"posts_answers" also has an "owner_user_id" column which specifies the ID of the user who answered the question. We want to arrange users in order of number of questions answered on the topic of "bigquery"


```python
# a SQL query that gets a list of users who have answered "bigquery"-related questions and the count of their answers.
# A single row for each user who answered. Results will have two columns:
# user_id - contains the owner_user_id column from the posts_answers table
# number_of_answers - contains the number of answers the user has written to "bigquery"-related questions

bigquery_experts_query = """
                         SELECT a.owner_user_id AS user_id, COUNT(1) AS number_of_answers
                         FROM `bigquery-public-data.stackoverflow.posts_questions` AS q 
                         INNER JOIN `bigquery-public-data.stackoverflow.posts_answers` AS a
                         ON q.id = a.parent_id
                         WHERE q.tags LIKE '%bigquery%'
                         GROUP BY user_id
                         ORDER BY number_of_answers DESC
                         """

# Set up the query
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
bigquery_experts_query_job = client.query(bigquery_experts_query, job_config=safe_config)

# API request - run the query, and return a pandas DataFrame
bigquery_experts_results = bigquery_experts_query_job.to_dataframe() 

# Preview results
print(bigquery_experts_results.head())
```

         user_id  number_of_answers
    0  5221944.0               3418
    1  1144035.0                989
    2   132438.0                898
    3  6253347.0                736
    4  1366527.0                620
    

These are the top answering users on the topic of "bigquery" on Stack Overflow.

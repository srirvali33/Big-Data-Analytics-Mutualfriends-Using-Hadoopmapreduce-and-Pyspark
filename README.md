# Big-Data-Analytics-Mutualfriends-Using-Hadoopmapreduce-and-Pyspark
A Bigdata project which analyzes social network data using "Hadoop mapreduce" and same with "Apache spark with python".

* If two people are friend then they have a lot of mutual/common friends. This program will find the common/mutual friend list for them.
For example,
Alice’s friends are Bob, Sam, Sara, Nancy
Bob’s friends are Alice, Sam, Clara, Nancy
Sara’s friends are Alice, Sam, Clara, Nancy

As Alice and Bob are friend and so, their mutual friend list is [Sam, Nancy]
As Sara and Bob are not friend and so, their mutual friend list is empty. (This case is excluded from output). 

Input:
Input files 

1. soc-LiveJournal1Adj.txt
The input contains the adjacency list and has multiple lines in the following format:
<User><TAB><Friends>

2. userdata.txt 
The userdata.txt contains dummy data which consist of 
column1 : userid
column2 : firstname
column3 : lastname
column4 : address
column5: city
column6 :state
column7 : zipcode
column8 :country
column9 :username
column10 : date of birth.
Output the following users with following pair are shown
(0,1), (20, 28193), (1, 29826), (6222, 19272), (28041, 28056)

![myimage-alt-tag](https://github.com/srirvali33/Big-Data-Analytics-Mutualfriends-Using-Hadoopmapreduce-and-Pyspark/blob/master/mutualfriendspic.jpg)

* Also friend pairs whose common friend number are within the top-10 in all the pairs are found.
Output Format:
<User_A>, <User_B><TAB><Mutual/Common Friend Number>

![myimage-alt-tag](https://github.com/srirvali33/Big-Data-Analytics-Mutualfriends-Using-Hadoopmapreduce-and-Pyspark/blob/master/top10friendspic.jpg)




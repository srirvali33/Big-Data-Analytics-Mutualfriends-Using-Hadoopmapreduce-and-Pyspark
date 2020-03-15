from pyspark import SparkContext
from pyspark import SparkConf


def make_pairs(linetext):
    user1 = linetext[0].strip()
    friend_list = linetext[1]
    if user1 != '':
        all_pairs = []
        for friend in friend_list:
            friend = friend.strip()
            if friend != '':
                if float(friend) < float(user1):
                    pairs = (friend + "," + user1, set(friend_list))
                else:
                    pairs = (user1 + "," + friend, set(friend_list))
                all_pairs.append(pairs)
        return all_pairs


def join(linetext):
    user1_fname  =linetext[1][1][0]
    user1_lname = linetext[1][1][1]
    user1_addr = linetext[1][1][2]
    new_key = linetext[1][0][0]
    common_friends= linetext[1][0][1]
    return new_key,((user1_fname,user1_lname,user1_addr),common_friends)


def final_result(linetext):
    usr1_fname=linetext[1][0][0][0]
    usr1_lname=linetext[1][0][0][1]
    usr2_fname=linetext[1][1][0]
    usr2_lname=linetext[1][1][1]
    usr2_addr=linetext[1][1][2]
    usr1_addr=linetext[1][0][0][2]
    comm_friends=linetext[1][0][1]
    return "{}\t{}\t{}\t{}\t{}\t{}\t{}".format(comm_friends,usr1_fname,usr1_lname,usr1_addr,usr2_fname,usr2_lname,usr2_addr)


if __name__ == "__main__":
    conf = SparkConf().setMaster("local").setAppName("quest2")
    sc = SparkContext(conf=conf)
    user_info = sc.textFile("userdata.txt").map(lambda eachline: eachline.split(","))
    user_details_cleaned=user_info.map(lambda x: (x[0],(x[1],x[2],x[3])))
    friends = sc.textFile("soc-LiveJournal1Adj.txt").map(lambda x: x.split("\t")).filter(lambda x: len(x) == 2).map(lambda x: [x[0], x[1].split(",")])
    friend_pairs = friends.flatMap(make_pairs)
    common_friends_found = friend_pairs.reduceByKey(lambda x, y: x.intersection(y))
    x=common_friends_found.map(lambda x:(x[0].split(",")[0],(x[0].split(",")[1],len(x[1]))))
    top_10friends=x.top(10,key=lambda x: x[1][1])
    y=sc.parallelize(top_10friends)
    joined_tab1 =y.join(user_details_cleaned)
    join_tab_formatted=joined_tab1.map(join)
    final_join=join_tab_formatted.join(user_details_cleaned)
    final_result_formatted=final_join.map(final_result)
    final_result_formatted.coalesce(1).saveAsTextFile("quest2.txt")
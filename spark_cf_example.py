
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("cf_example").getOrCreate()

pre_cf = spark.read.csv('/sample/path/for/data/pre_cf_sample.csv', header=True)
pre_cf.show(20, False)

# create new variable for prob. of item one int. item two
pre_cf = pre_cf.withColumn('int_prob', pre_cf.item_one_int_item_two / pre_cf.tot_baskets)
# # create new variable for unconditional probability of item 2
pre_cf = pre_cf.withColumn('item_two_prob', pre_cf.item_two_freq / pre_cf.tot_baskets)
# we now have pr(a int. b) and pr(b); create new conditional prob variable
pre_cf = pre_cf.withColumn('cond_prob', pre_cf.int_prob / pre_cf.item_two_prob)
pre_cf.show(15, False)

## pb filter 
# limit down to item_one=pb, keep just a handful of columns, sort by one of them
pb_df = pre_cf.filter(pre_cf.item_one=='pb').select(['item_one', 'item_two', 'cond_prob']).sort('cond_prob', ascending=False)
pb_df.show()

## kale filter
# same as above
kale_df = pre_cf.filter(pre_cf.item_one=='kale').select(['item_one', 'item_two', 'cond_prob']).sort('cond_prob', ascending=False)
kale_df.show()





在之前的学习中，我们已经学习了 Spark 的基础编程方式，接下来，我们看看在实际的工作中如何使用这些 API 实现具体的需求。这些需求是电商网站的真实需求，所以在实现功能前，咱们必须先将数据准备好。
![image](https://user-images.githubusercontent.com/84574962/123816057-957fcf80-d929-11eb-9bcf-0dd9770a97a7.png)
上面的数据图是从数据文件中截取的一部分内容，表示为电商网站的用户行为数据，主要包含用户的 4 种行为：搜索，点击，下单，支付。数据规则如下：
➢数据文件中每行数据采用下划线分隔数据
➢每一行数据表示用户的一次行为，这个行为只能是 4 种行为的一种
➢如果搜索关键字为 null,表示数据不是搜索数据
➢如果点击的品类 ID 和产品 ID 为-1，表示数据不是点击数据
➢针对于下单行为，一次可以下单多个商品，所以品类 ID 和产品 ID 可以是多个，id 之间采用逗号分隔，如果本次不是下单行为，则数据采用 null 表示
➢支付行为和下单行为类似
详细字段说明：
![image](https://user-images.githubusercontent.com/84574962/123816242-bb0cd900-d929-11eb-9787-3067dbe241a9.png)
![image](https://user-images.githubusercontent.com/84574962/123816276-c2cc7d80-d929-11eb-9bac-e29de6ebf438.png)
样例类：
//用户访问动作表
case class UserVisitAction(
date: String,//用户点击行为的日期
user_id: Long,//用户的 ID
session_id: String,//Session 的 ID
page_id: Long,//某个页面的 ID
action_time: String,//动作的时间点
search_keyword: String,//用户搜索的关键词
click_category_id: Long,//某一个商品品类的 ID
click_product_id: Long,//某一个商品的 ID
order_category_ids: String,//一次订单中所有品类的 ID 集合
order_product_ids: String,//一次订单中所有商品的 ID 集合
pay_category_ids: String,//一次支付中所有品类的 ID 集合
pay_product_ids: String,//一次支付中所有商品的 ID 集合
city_id: Long
)//城市 id

需求 1：Top10 热门品类
![image](https://user-images.githubusercontent.com/84574962/123816366-d546b700-d929-11eb-8e34-421b1eebe77c.png)
1.1 需求说明
品类是指产品的分类，大型电商网站品类分多级，咱们的项目中品类只有一级，不同的公司可能对热门的定义不一样。我们按照每个品类的点击、下单、支付的量来统计热门品类。

鞋
点击数 下单数 支付数

衣服
点击数 下单数 支付数

电脑
点击数 下单数 支付数
例如，综合排名 = 点击数*20%+下单数*30%+支付数*50%
项目需求优化为：先按照点击数排名，靠前的就排名高；如果点击数相同，再比较下单数；下单数再相同，就比较支付数。

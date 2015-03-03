##MapReduce实现电影推荐系统
###案例分析
* 互联网某电影点评网站，主要产品包括
    * 电影介绍
    * 电影排行
    * **网友对电影打分**
    * 网友影评
    * 影讯&购票
    * 用户在看|想看|看过的电影
    * **猜你喜欢（推荐）**
* 利用用户对电影的打分表来给用户推荐电影，用户打分表包括以下字段
    * userID--用户ID号
    * itemID--电影ID号
    * score--评分

###基于电影的协同过滤算法
* 建立物品的同现矩阵  
  ![image](https://github.com/ParadeTo/Recommend/tree/master/img/theory-5.png)
* 建立用户对物品的评分矩阵  
  ![image](https://github.com/ParadeTo/Recommend/tree/master/img/theory-6.png)
* 矩阵计算推荐结果  
  ![image](https://github.com/ParadeTo/Recommend/tree/master/img/theory-7.png)

###MapReduce实现
* 程序流程图  
 ![image](https://github.com/ParadeTo/Recommend/tree/master/mapreduce.jpg)
* Java类说明  
Recommend.java--主任务启动程序  
Step1.java--按用户分组，计算所有物品出现的组合列表，得到用户对物品的评分矩阵  
Step2.java--对itemID组合列表进行计数，建立其同现矩阵  
Step3.java--对同现矩阵和评分矩阵进行转型，便于后续处理  
Step4.

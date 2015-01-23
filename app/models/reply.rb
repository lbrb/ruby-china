# coding: utf-8
require "digest/md5"
class Reply
  include Mongoid::Document
  include Mongoid::Timestamps
  include Mongoid::BaseModel
  include Mongoid::CounterCache
  include Mongoid::SoftDelete
  include Mongoid::MarkdownBody
  include Mongoid::Mentionable
  include Mongoid::Likeable

  field :body
  field :body_html
  field :source
  field :message_id

  belongs_to :user, inverse_of: :replies
  belongs_to :topic, inverse_of: :replies, touch: true
  has_many :notifications, class_name: 'Notification::Base', dependent: :delete

  counter_cache name: :user, inverse_of: :replies
  counter_cache name: :topic, inverse_of: :replies

  index user_id: 1
  index topic_id: 1

  delegate :title, to: :topic, prefix: true, allow_nil: true
  delegate :login, to: :user, prefix: true, allow_nil: true

  validates_presence_of :body
  validates_uniqueness_of :body, scope: [:topic_id, :user_id], message: "不能重复提交。"
  validate do
    ban_words = (SiteConfig.ban_words_on_reply || "").split("\n").collect { |word| word.strip }
    if self.body.strip.downcase.in?(ban_words)
      self.errors.add(:body, "请勿回复无意义的内容，如你想收藏或赞这篇帖子，请用帖子后面的功能。")
    end
  end

  after_save :update_parent_topic

  def update_parent_topic
    topic.update_last_reply(self)
  end

  # 删除的时候也要更新 Topic 的 updated_at 以便清理缓存
  after_destroy :update_parent_topic_updated_at

  def update_parent_topic_updated_at
    if not self.topic.blank?
      self.topic.update_deleted_last_reply(self)
      true
    end
  end


  after_create do
    Reply.delay.send_topic_reply_notification(self.id)
  end

  def self.per_page
    50
  end

  def self.send_topic_reply_notification(reply_id)
    reply = Reply.find_by_id(reply_id)
    return if reply.blank?
    topic = Topic.find_by_id(reply.topic_id)
    return if topic.blank?

    notified_user_ids = reply.mentioned_user_ids

    # 给发帖人发回帖通知
    if reply.user_id != topic.user_id && !notified_user_ids.include?(topic.user_id)
      Notification::TopicReply.create user_id: topic.user_id, reply_id: reply.id
      notified_user_ids << topic.user_id
    end

    # 给关注者发通知
    topic.follower_ids.each do |uid|
      # 排除同一个回复过程中已经提醒过的人
      next if notified_user_ids.include?(uid)
      # 排除回帖人
      next if uid == reply.user_id
      puts "Post Notification to: #{uid}"
      Notification::TopicReply.create user_id: uid, reply_id: reply.id
    end
    true
  end

  # 是否热门
  def popular?
    self.likes_count >= 5
  end

  def destroy
    super
    notifications.delete_all
    delete_notifiaction_mentions
  end

  #从mongodb中取出特定时间段内(特定时间段可以自己设定)，每个topic的加权score.
  #返回值 like [{"_id"=>1.0, "value"=>1.0}, {"_id"=>2.0, "value"=>1.0}, {"_id"=>3.0, "value"=>14.0}], _id是topic.id, value为加权score
  #divisor:除数，根据回复创建时间与当前时间的毫秒数，算出该条回复属于哪段时间
  #period_length: 周期长度，例如一周热门，period_length = 7, 24小时热门，period_length =24
  def self.get_hot_topic_from_mongodb(period)
    case period
      when 'week' then
        divisor = 3600000*24
        period_length = 7
      when 'day' then
        divisor = 3600000
        period_length = 24
      else
        divisor = 1
        period_length = 1
    end
    map = ' function(){
              emit(this.topic_id, this.created_at);
            }
           '
    #处理map后values为数组的情况
    reduce = %| function(key, values){
                  var score = 0;
                  var v;
                  var off_days;
                  var now = new Date();
                  for(v in values){
                    off_days = parseInt((now-values[v])/#{divisor});
                    score += (#{period_length}-off_days);
                  }
                  return score;
                }
              |

    #处理map后values为为单一元素的情况
    finalize = %| function(key, value){
                    var off_days;
                    if(typeof value == 'object'){
                      off_days = parseInt((now-value)/#{divisor});
                      value = (#{period_length}-off_days);
                    }
                    return value;
                  }
                |
    map_reduce(map, reduce).finalize(finalize).out(inline: true)
  end

  #得到特定时间段内的热门帖子并缓存到redis，并返回热门topic的数组
  #redis_key: redis 缓存的key值
  #redis_expire： redis 缓存的过期时间
  def self.get_hot_topic(period)
    redis_key = "#{period}_hot_topic"
    redis_expire = case period
                     when 'week' then
                       3600
                     when 'day' then
                       600
                   end
    unless $redis.exists redis_key
      topic_with_score = self.where(:created_at.gt => 1.send(period.to_sym).ago).get_hot_topic_from_mongodb(period).map { |reply| [reply['value'], reply['_id']] }

      $redis.zadd redis_key, topic_with_score
      $redis.expire redis_key, redis_expire
    end
    hot_topic_ids = $redis.zrevrange 'week_hot_topic', 0, 4
    Topic.find(hot_topic_ids).to_a
  end
end

# 访问地址
bind = "0.0.0.0:8802"
# 工作进程数
workers = 8
# 超时时间
timeout = 300
# 输出日志级别
loglevel = 'error'
# 存放日志路径
pidfile = "log/gunicorn.pid"
# 存放日志路径
accesslog = "log/access.log"
# 存放日志路径
errorlog = "log/debug.log"
#工作模式
worker_class='gevent'
#gevent模式下的连接数，最大并发=workers*worker-connections
worker_connections=100

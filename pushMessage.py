import json
import requests


def getToken():
    # r = requests.get("https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid=ww933837de46f128d4&corpsecret="
    #                  "GsSLQccsEacUs-q2RxYud_UrWcTPZoCoW-xurHZN0_M")
    url = "https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid=ww933837de46f128d4&corpsecret=" \
          "GsSLQccsEacUs-q2RxYud_UrWcTPZoCoW-xurHZN0_M"
    try:
        s = requests.session()
        s.keep_alive = False
        s.adapters.DEFAULT_RETRIES = 100
        r = s.get(url, timeout=6)  # 获得请求数据
    except requests.exceptions.ConnectionError as e:
        print('error', e.args)
    else:
        if r.status_code == 200:
            data = json.loads(r.text)
            if data["errcode"] == 0:
                return data["access_token"]
                pass
            else:
                return str(data["errcode"]) + "parameter error"
        else:
            return "network failure"  # 连接服务器失败


def fun_push_message(content):
    data = {
           "touser": "CaiPan",
           "msgtype": "text",
           "agentid": 1000002,
           "text": {
               "content": str(content)
           },
           "safe": 0
        }
    access_token = getToken()
    try:
        s = requests.session()
        s.keep_alive = False
        s.adapters.DEFAULT_RETRIES = 100
        # r = s.get(url, timeout=6)  # 获得请求数据
        r = s.post("https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={}".format(access_token),
                   data=json.dumps(data), timeout=6)
        print(r.text)
    except requests.exceptions.ConnectionError as e:
        print('error', e.args)
    except Exception:
        print("未知错误")

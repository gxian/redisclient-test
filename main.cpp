#include "pistache-promise/async.h"
#include "redisclient/redisasyncclient.h"
#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/address.hpp>
#include <chrono>
#include <cinttypes>
#include <ctime>
#include <iostream>
#include <ratio>
#include <thread>

class RedisClientPool {
public:
    RedisClientPool(std::shared_ptr<boost::asio::io_service> ios,
                    std::string pw, std::string host = std::string("127.0.0.1"),
                    int size = 64, int port = 6379)
        : ios_(ios), host_(host), pw_(pw), port_(6379), size_(64) {}

    void Connect(std::function<void(bool)> cb) {
        pool_.reserve(size_);
        auto counter = new int(size_);
        auto report = [cb, counter](boost::system::error_code ec) {
            static int success = *counter;
            if (!ec) {
                --success;
            }
            if (--(*counter) == 0) {
                delete counter;
                if (success == 0) {
                    cb(true);
                } else {
                    cb(false);
                }
            }
        };
        for (int i = 0; i < size_; ++i) {
            std::shared_ptr<redisclient::RedisAsyncClient> cli;
            RedisConnector(cli, report);
            pool_.push_back(std::move(cli));
        }
    }

    Pistache::Async::Promise<std::deque<std::string>>
    PCommand(std::string cmd, std::deque<std::string> args) {
        struct Wrap {
            Pistache::Async::Resolver resolve;
            Pistache::Async::Rejection reject;
            Wrap(Pistache::Async::Resolver res, Pistache::Async::Rejection rej)
                : resolve(std::move(res)), reject(std::move(rej)) {}
        };
        return Pistache::Async::Promise<std::deque<std::string>>(
            [&, this](Pistache::Async::Resolver &resolve,
                      Pistache::Async::Rejection &reject) {
                std::shared_ptr<Wrap> w(
                    new Wrap(std::move(resolve), std::move(reject)));
                auto cb = [](bool res, std::deque<std::string> vals,
                             std::shared_ptr<Wrap> sp) {
                    if (res) {
                        sp->resolve(vals);
                    } else {
                        sp->reject(vals);
                    }
                };
                auto wrap_cb = std::bind(cb, std::placeholders::_1,
                                         std::placeholders::_2, w);
                Command(cmd, args, wrap_cb);
            });
    }

    bool Command(std::string cmd, std::deque<std::string> args,
                 std::function<void(bool, std::deque<std::string>)> cb) {
        static uint16_t cursor = 0;
        auto cli = pool_[++cursor % size_];
        if (!cli->isConnected()) {
            return false;
        }
        std::deque<redisclient::RedisBuffer> bufs;
        for (auto &i : args) {
            bufs.push_back(i);
        }
        auto handler = [cmd, args, cb](redisclient::RedisValue val) {
            try {
                std::deque<std::string> vals;
                if (val.isOk()) {
                    auto append = [&vals](redisclient::RedisValue &val) {
                        if (val.isString()) {
                            vals.push_back(val.toString());
                        } else if (val.isInt()) {
                            vals.push_back(std::to_string(val.toInt()));
                        } else if (val.isNull()) {
                            // nothing
                        } else {
                        }
                    };
                    if (val.isArray()) {
                        for (auto &i : val.toArray()) {
                            append(i);
                        }
                    } else {
                        append(val);
                    }
                } else {
                }

                cb(val.isOk(), vals);
            } catch (const std::exception &e) {
                cb(false, std::deque<std::string>());
            }
        };
        cli->command(cmd, std::ref(bufs), handler);
        return true;
    }

    void RedisConnector(std::shared_ptr<redisclient::RedisAsyncClient> &client,
                        std::function<void(boost::system::error_code)> cb) {
        client.reset(new redisclient::RedisAsyncClient(*ios_));
        if (client->isConnected()) {
            client->disconnect();
        }
        auto callback = cb;
        if (!pw_.empty()) {
            auto auth =
                [this,
                 cb](boost::system::error_code ec,
                     std::shared_ptr<redisclient::RedisAsyncClient> client) {
                    if (!ec) {
                        client->command(
                            "AUTH", {pw_},
                            [ec, cb](redisclient::RedisValue val) {
                                if (val.isError()) {
                                    cb(boost::system::error_code());
                                } else {
                                    cb(boost::system::error_code(
                                        0, boost::system::system_category()));
                                }

                            });
                    } else {
                        cb(ec);
                    }
                };
            callback = std::bind(auth, std::placeholders::_1, client);
        }
        // 解析endpoint
        boost::asio::ip::tcp::resolver resolver(*ios_);
        boost::asio::ip::tcp::resolver::query query(host_,
                                                    std::to_string(port_));
        boost::asio::ip::tcp::resolver::iterator iter = resolver.resolve(query);
        boost::asio::ip::tcp::endpoint ep = *iter;

        client->installErrorHandler([this, &client,
                                     callback](const std::string &error) {
            if (client->isConnected()) {
                client->disconnect();
            }
            RedisConnector(client, [this, &client,
                                    callback](boost::system::error_code ec) {
                // OnRedisReconnect(ec, client, callback);
            });
        });
        client->connect(ep, callback);
    }

    void
    OnRedisReconnect(boost::system::error_code ec,
                     std::shared_ptr<redisclient::RedisAsyncClient> &client,
                     std::function<void(boost::system::error_code)> cb) {
        if (ec) {
            //     StartTimer(kReconnectTime, [this, &client, cb]() {
            //         RedisConnector(
            //             client, [this, &client, cb](boost::system::error_code
            //             ec) {
            //                 OnRedisReconnect(ec, client, cb);
            //             });
            //     });
        } else {
            cb(ec);
        }
    }

private:
    std::shared_ptr<boost::asio::io_service> ios_;
    std::vector<std::shared_ptr<redisclient::RedisAsyncClient>> pool_;
    std::string host_;
    std::string pw_;
    int port_;
    uint16_t size_;
};

void seq_test(RedisClientPool &pool, int total, std::function<void()> counter) {
    for (int i = 0; i < total; ++i) {
        // std::cout << "set key: " << i << std::endl;
        pool.PCommand("SET", {std::to_string(i), std::to_string(i)})
            .then(
                [&pool, i, counter](std::deque<std::string> res) {
                    // counter();
                    return pool.PCommand(
                        "SET", {std::to_string(i), std::to_string(i)});
                },
                [counter](std::exception_ptr ptr) {})
            .then(
                [&pool, i, counter](std::deque<std::string> res) { counter(); },
                [counter](std::exception_ptr ptr) {});

        // pool.Command("SET", {std::to_string(i), std::to_string(i)},
        //              [i, counter](bool res, std::deque<std::string> vals) {
        //                  counter();
        //              });

        // std::string script =
        //     R"lua( return {redis.call("set", KEYS[1], KEYS[1]), KEYS[1]}
        //     )lua";
        // pool.Command(
        //     "EVAL", {script, "1", std::to_string(i)},
        //     // pool.Command("SET", {std::to_string(i), std::to_string(i)},
        //     [i, counter](bool res, std::deque<std::string> vals) {
        //         std::cout << "on key: " << i << " set, res: " << res
        //                   << " check: " << std::atoi(vals[1].c_str())
        //                   << std::endl;
        //         counter();
        //     });
        // std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
}

static const char *kKeyIndexMem = "guild_service/app/35001/index/member";
static const char *kKeyUserStatus = "guild_service/app/35001/user/status";
static const char *kKeyGuildMember =
    "guild_service/app/35001/guild/16017/member";
static const char *kLuaGetUser = R"lua(
local member_list_str = redis.call("HVALS", KEYS[1])
local ret = {}
ret["status"] = 0
ret["members"] = {}
for k, v in pairs(member_list_str) do
    local member = cjson.decode(v)
    local info = {}
    info["id"] = member["user"]
    info["name"] = member["name"]
    info["img"] = member["img"]
    local state = redis.call("hget", KEYS[2], member["user"])
    if (state == false) then
        state = 0
    end
    info["state"] = state
    table.insert(ret["members"], info)
    break
end
return {0, cjson.encode(ret)}
)lua";

void guild_member_test(RedisClientPool &pool, int total,
                       std::function<void()> counter) {
    for (int i = 0; i < total; ++i) {
        // auto procomm = Pistache::Async::Promise<std::pair<bool,
        // std::deque<std::string>>();
        pool.Command(
            "HGET", {kKeyIndexMem, "4404454"},
            [&pool, counter](bool res, std::deque<std::string> vals) {
                if (res) {
                    pool.Command(
                        "EVAL",
                        {kLuaGetUser, "2", kKeyGuildMember, kKeyUserStatus},
                        [counter](bool res, std::deque<std::string> vals) {
                            // std::cout << vals[1] << std::endl;
                            counter();
                        });
                }
            });
    }
}

int main(int argc, char *argv[]) {
    if (argc != 5) {
        return 1;
    }
    // Pistache::Async::Promise<void> prom([=](Pistache::Async::Resolver&
    // resolve, Pistache::Async::Rejection& reject) {});
    auto total = static_cast<int>(std::atoi(argv[1]));
    auto conn = static_cast<int>(std::atoi(argv[2]));
    std::string pw = argv[3];
    std::string host = argv[4];
    auto ios =
        std::shared_ptr<boost::asio::io_service>(new boost::asio::io_service);

    using namespace std::chrono;
    high_resolution_clock::time_point t1 = high_resolution_clock::now();
    auto counter = [ios, total, t1]() {
        static int cnt = total;
        --cnt;
        if (cnt == 0) {
            high_resolution_clock::time_point t2 = high_resolution_clock::now();
            duration<double> time_span =
                duration_cast<duration<double>>(t2 - t1);
            std::cout << "time cost: " << time_span.count() << std::endl;
            ios->stop();
        }
    };
    RedisClientPool pool(ios, pw, host, conn);
    pool.Connect([&pool, total, counter](bool res) {
        std::cout << "connect result: " << res << std::endl;
        // guild_member_test(pool, total, counter);
        seq_test(pool, total, counter);
    });
    ios->run();
    return 0;
}
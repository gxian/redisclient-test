#include "redisclient/redisasyncclient.h"
#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/address.hpp>

class RedisClientPool {
public:
    void RedisConnector(std::shared_ptr<redisclient::RedisAsyncClient> &client,
                        std::function<void(boost::system::error_code)> cb) {
        client.reset(new redisclient::RedisAsyncClient(*ios_));
        if (client->isConnected()) {
            client->disconnect();
        }
        auto callback = cb;
        if (!pw_.empty()) {
            auto auth = [this, &client, cb](boost::system::error_code ec) {
                if (!ec) {
                    client->command(
                        "AUTH", {pw_}, [ec, cb](redisclient::RedisValue val) {
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
            callback = auth;
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
        //             client, [this, &client, cb](boost::system::error_code ec) {
        //                 OnRedisReconnect(ec, client, cb);
        //             });
        //     });
        } else {
            cb(ec);
        }
    }

private:
    std::shared_ptr<boost::asio::io_service> ios_;
    std::string host_;
    std::string pw_;
    int port_;
};

int main(int argc, char *argv[]) { return 0; }
#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>
#include <vector>
#include <map>
#include <set>
#include <string>

// Function to parse command line arguments

bool parseArgs(int argc, char* argv[],
               std::string& r_name,
               std::string& start_date,
               std::string& end_date,
               int& num_threads,
               std::string& table_path,
               std::string& result_path)
{
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];

        if (arg == "--r_name" && i + 1 < argc) {
            r_name = argv[++i];
        }
        else if (arg == "--start_date" && i + 1 < argc) {
            start_date = argv[++i];
        }
        else if (arg == "--end_date" && i + 1 < argc) {
            end_date = argv[++i];
        }
        else if (arg == "--threads" && i + 1 < argc) {
            num_threads = std::stoi(argv[++i]);
        } 
        else if (arg == "--table_path" && i + 1 < argc) {
            table_path = argv[++i];
        }
        else if (arg == "--result_path" && i + 1 < argc) {
            result_path = argv[++i];
        }
        else {
            return false;
        }
    }
                  
    return !(r_name.empty() ||
             start_date.empty() ||
             end_date.empty() ||
             table_path.empty() || 
             result_path.empty());
}
//Function to read TPCH data from the specified path
bool readTPCHData(
    const std::string& table_path,
    std::vector<std::map<std::string, std::string>>& customer_data,
    std::vector<std::map<std::string, std::string>>& orders_data,
    std::vector<std::map<std::string, std::string>>& lineitem_data,
    std::vector<std::map<std::string, std::string>>& supplier_data,
    std::vector<std::map<std::string, std::string>>& nation_data,
    std::vector<std::map<std::string, std::string>>& region_data)
{
    auto readTable = [&](const std::string& file,
                         const std::vector<std::string>& columns,
                         std::vector<std::map<std::string, std::string>>& out) -> bool
    {
        std::ifstream fin(file);
        if (!fin.is_open()) return false;

        std::string line;
        while (std::getline(fin, line)) {
            std::stringstream ss(line);
            std::string token;
            std::map<std::string, std::string> row;

            for (const auto& col : columns) {
                if (!std::getline(ss, token, '|'))
                    return false;
                row[col] = token;
            }
            out.push_back(row);
        }
       std::cout << file << "rows read: " << out.size() << std::endl;
        return true;
    };

    return
        readTable(table_path + "/customer.tbl",
                  {"c_custkey", "c_name", "c_address", "c_nationkey"},
                  customer_data) &&

        readTable(table_path + "/orders.tbl",
                  {"o_orderkey", "o_custkey", "o_orderdate"},
                  orders_data) &&

        readTable(table_path + "/lineitem.tbl",
                  {"l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"},
                  lineitem_data) &&

        readTable(table_path + "/supplier.tbl",
                  {"s_suppkey", "s_name", "s_nationkey"},
                  supplier_data) &&

        readTable(table_path + "/nation.tbl",
                  {"n_nationkey", "n_name", "n_regionkey"},
                  nation_data) &&

        readTable(table_path + "/region.tbl",
                  {"r_regionkey", "r_name"},
                  region_data);
}
        
       
    




  


// Function to execute TPCH Query 5 using multithreading
bool executeQuery5(
    const std::string& r_name,
    const std::string& start_date,
    const std::string& end_date,
    int num_threads,
    const std::vector<std::map<std::string, std::string>>& customer_data,
    const std::vector<std::map<std::string, std::string>>& orders_data,
    const std::vector<std::map<std::string, std::string>>& lineitem_data,
    const std::vector<std::map<std::string, std::string>>& supplier_data,
    const std::vector<std::map<std::string, std::string>>& nation_data,
    const std::vector<std::map<std::string, std::string>>& region_data,
    std::map<std::string, double>& result)
{
    // Step 1: region filter
    std::set<std::string> valid_region_keys;
    for (const auto& r : region_data) {
        if (r.at("r_name") == r_name) {
            valid_region_keys.insert(r.at("r_regionkey"));
        }
    }

    // Step 2: nation filter
    std::map<std::string, std::string> nation_to_name;
    for (const auto& n : nation_data) {
        if (valid_region_keys.count(n.at("n_regionkey"))) {
            nation_to_name[n.at("n_nationkey")] = n.at("n_name");
        }
    }

    // Step 3: supplier filter
    std::map<std::string, std::string> supplier_to_nation;
    for (const auto& s : supplier_data) {
        if (nation_to_name.count(s.at("s_nationkey"))) {
            supplier_to_nation[s.at("s_suppkey")] = s.at("s_nationkey");
        }
    }

    // Step 4: customer → nation
    std::map<std::string, std::string> customer_to_nation;
    for (const auto& c : customer_data) {
        if (nation_to_name.count(c.at("c_nationkey"))) {
            customer_to_nation[c.at("c_custkey")] = c.at("c_nationkey");
        }
    }

    // Step 5: orders filter by date
    std::map<std::string, std::string> order_to_customer;
    for (const auto& o : orders_data) {
        const std::string& date = o.at("o_orderdate");
        if (date >= start_date && date < end_date) {
            order_to_customer[o.at("o_orderkey")] = o.at("o_custkey");
        }
    }

    std::mutex mtx;
    size_t total = lineitem_data.size();
    size_t chunk = total / num_threads;

    auto worker = [&](size_t start, size_t end) {
        std::map<std::string, double> local_result;

        for (size_t i = start; i < end; i++) {
            const auto& l = lineitem_data[i];

            auto it_order = order_to_customer.find(l.at("l_orderkey"));
            if (it_order == order_to_customer.end()) continue;

            auto it_supp = supplier_to_nation.find(l.at("l_suppkey"));
            if (it_supp == supplier_to_nation.end()) continue;

            auto it_cust = customer_to_nation.find(it_order->second);
            if (it_cust == customer_to_nation.end()) continue;

            if (it_supp->second != it_cust->second) continue;

            double price = std::stod(l.at("l_extendedprice"));
            double discount = std::stod(l.at("l_discount"));

            std::string nation = nation_to_name[it_supp->second];
            local_result[nation] += price * (1.0 - discount);
        }

        std::lock_guard<std::mutex> lock(mtx);
        for (const auto& p : local_result) {
            result[p.first] += p.second;
        }
    };

    // Launch threads
    std::vector<std::thread> threads;
    size_t start = 0;

    for (int i = 0; i < num_threads; i++) {
        size_t end = (i == num_threads - 1) ? total : start + chunk;
        threads.emplace_back(worker, start, end);
        start = end;
    }

    for (auto& t : threads) {
        t.join();
    }

    return true;
}



// Function to output results to the specified path
 bool outputResults(const std::string& result_path, const std::map<std::string, double>& result) {
    // Copy map to vector for sorting
    std::vector<std::pair<std::string, double>> sorted_result(result.begin(), result.end());

    // Sort descending by revenue
    std::sort(sorted_result.begin(), sorted_result.end(),
              [](const std::pair<std::string, double>& a, const std::pair<std::string, double>& b) {
                  return a.second > b.second;
              });

    // Open output file
    std::ofstream outfile(result_path);
    if (!outfile.is_open()) {
        std::cerr << "Failed to open result file: " << result_path << std::endl;
        return false;
    }

    // Write header
    outfile << "n_name|revenue\n";

    // Write results
    for (const auto& p : sorted_result) {
        outfile << p.first << "|" << p.second << "\n";
    }

    outfile.close();
    return true;
}
   


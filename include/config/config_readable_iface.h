#pragma once

#include <boost/algorithm/string/predicate.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/property_tree/ptree.hpp>

#include <new>
#include <string>
#include <type_traits>

namespace config {

using section_type = boost::property_tree::ptree;
using config_tree_type = boost::property_tree::ptree;

class IConfigReadable {
public:
    virtual ~IConfigReadable() = default;

    virtual const section_type& GetSection(const std::string& section_name) const = 0;
    virtual std::string GetParameter(const std::string& section_name,
                                     const std::string& parameter_name,
                                     const std::string& default_value) const = 0;
};

template <class T>
T ReadParameter(const IConfigReadable& config,
                const std::string& section_name,
                const std::string& parameter_name,
                const T& default_value) {
    if constexpr (std::is_same_v<T, std::string>) {
        return config.GetParameter(section_name, parameter_name, default_value);
    } else {
        const auto value = config.GetParameter(section_name, parameter_name, {});
        if (value.empty()) {
            return default_value;
        }

        try {
            if constexpr (std::is_same_v<T, bool>) {
                if (value == "1" || boost::iequals(value, "true") || boost::iequals(value, "yes") ||
                    boost::iequals(value, "on")) {
                    return true;
                }
                if (value == "0" || boost::iequals(value, "false") || boost::iequals(value, "no") ||
                    boost::iequals(value, "off")) {
                    return false;
                }
                return default_value;
            } else {
                return boost::lexical_cast<T>(value);
            }
        } catch (const std::bad_alloc&) {
            throw;
        } catch (...) {
            return default_value;
        }
    }
}

}  // namespace config

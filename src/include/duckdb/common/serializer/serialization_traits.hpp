#pragma once
#include <type_traits>
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {


class FormatSerializer; // Forward declare
class FormatDeserializer; // Forward declare

// Backport to c++11
template <class...> using void_t = void;

// Check for anything implementing a `void FormatSerialize(FormatSerializer &FormatSerializer)` method
template <typename T, typename = void_t<>>
struct has_serialize : std::false_type {};

template <typename T>
struct has_serialize<T, void_t<decltype(std::declval<T>().FormatSerialize(std::declval<FormatSerializer &>()))>>
    : std::true_type {};

template <typename T>
constexpr bool has_serialize_v() {
	return has_serialize<T>::value;
}

// Check for anything implementing a static `T FormatDeserialize(FormatDeserializer&)` method
/*
template <typename T, typename = void_t<>>
struct has_deserialize : std::false_type {};

template <typename T>
struct has_deserialize<T, void_t<decltype(T::FormatDeserialize)>>
    : std::true_type {};

template <typename T>
constexpr bool has_deserialize_v() {
    return has_deserialize<T>::value;
}*/

template <typename T, typename = T>
struct has_deserialize : std::false_type {};

// Accept `static unique_ptr<T> FormatDeserialize(FormatDeserializer& deserializer)`
template <typename T>
struct has_deserialize<
    T, typename std::enable_if<std::is_same<decltype(T::FormatDeserialize), unique_ptr<T>(FormatDeserializer &)>::value,
                               T>::type> : std::true_type {};

// Accept `static shared_ptr<T> FormatDeserialize(FormatDeserializer& deserializer)`
template <typename T>
struct has_deserialize<
    T, typename std::enable_if<std::is_same<decltype(T::FormatDeserialize), shared_ptr<T>(FormatDeserializer &)>::value,
                               T>::type> : std::true_type {};

// Accept `static T FormatDeserialize(FormatDeserializer& deserializer)`
template <typename T>
struct has_deserialize<
    T, typename std::enable_if<std::is_same<decltype(T::FormatDeserialize), T(FormatDeserializer &)>::value, T>::type>
    : std::true_type {};

// Check if T is a vector, and provide access to the inner type
template <typename T>
struct is_vector : std::false_type {};
template <typename T>
struct is_vector<typename std::vector<T>> : std::true_type {
	typedef T inner_type;
};

// Check if T is a unordered map, and provide access to the inner type
template <typename ...Args>
struct is_unordered_map : std::false_type {};
template <typename ...Args>
struct is_unordered_map<typename std::unordered_map<Args...>> : std::true_type {
	typedef typename std::tuple_element<0, std::tuple<Args...>>::type key_type;
	typedef typename std::tuple_element<1, std::tuple<Args...>>::type value_type;
	typedef typename std::tuple_element<2, std::tuple<Args...>>::type hash_type;
	typedef typename std::tuple_element<3, std::tuple<Args...>>::type equal_type;
};

template <typename T>
struct is_unique_ptr : std::false_type
{};

template <typename T, typename D>
struct is_unique_ptr<typename std::unique_ptr<T, D>> : std::true_type
{
	typedef T inner_type;
	typedef D deleter_type;
};

template <typename T>
struct is_shared_ptr : std::false_type
{};

template <typename T>
struct is_shared_ptr<typename std::shared_ptr<T>> : std::true_type
{
	typedef T inner_type;
};

template <typename T>
struct is_pair : std::false_type
{};

template <typename T, typename U>
struct is_pair<std::pair<T, U>> : std::true_type
{
	typedef T first_type;
	typedef U second_type;
};

template <typename ...Args>
struct is_unordered_set : std::false_type
{};
template <typename ...Args>
struct is_unordered_set<typename std::unordered_set<Args...>> : std::true_type
{
	typedef typename std::tuple_element<0, std::tuple<Args...>>::type inner_type;
	typedef typename std::tuple_element<1, std::tuple<Args...>>::type hash_type;
	typedef typename std::tuple_element<2, std::tuple<Args...>>::type equal_type;
};

template <typename ...Args>
struct is_set : std::false_type
{};
template <typename ...Args>
struct is_set<typename std::set<Args...>> : std::true_type
{
	typedef typename std::tuple_element<0, std::tuple<Args...>>::type inner_type;
	typedef typename std::tuple_element<1, std::tuple<Args...>>::type hash_type;
	typedef typename std::tuple_element<2, std::tuple<Args...>>::type equal_type;
};


}
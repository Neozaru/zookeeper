#ifndef LOGGING_H_
#define LOGGING_H_

#include <boost/filesystem.hpp>
#include <boost/format.hpp>
#include <boost/version.hpp>
#include <log4cxx/logger.h>

#define LOG_DEBUG(message) LOG4CXX_DEBUG(logger, message)
#define LOG_INFO(message) LOG4CXX_DEBUG(logger, message)
#define LOG_WARN(message) LOG4CXX_DEBUG(logger, message)
#define LOG_ERROR(message) LOG4CXX_DEBUG(logger, message)

#if BOOST_VERSION / 100 % 1000 >= 46
#define ENABLE_LOGGING static log4cxx::LoggerPtr logger(\
  log4cxx::Logger::getLogger(\
  boost::filesystem::path(__FILE__).filename().string()));
#else
#define ENABLE_LOGGING static log4cxx::LoggerPtr logger(\
  log4cxx::Logger::getLogger(\
  boost::filesystem::path(__FILE__).filename()));
#endif

#endif  // LOGGING_H_

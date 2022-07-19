-- MySQL dump 10.13  Distrib 5.7.34, for osx10.15 (x86_64)
--
-- Host: 127.0.0.1    Database: ntd
-- ------------------------------------------------------
-- Server version	5.5.5-10.7.3-MariaDB-1:10.7.3+maria~focal

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `iu`
--

DROP TABLE IF EXISTS `iu`;
CREATE TABLE `iu` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `code` text DEFAULT NULL,
  PRIMARY KEY(id),
  UNIQUE KEY `uc_iu_code` (`code`) USING HASH
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- Table structure for table `disease`
--

DROP TABLE IF EXISTS `disease`;
CREATE TABLE `disease` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `type` varchar(16) NOT NULL,
  `species` varchar(16) NOT NULL,
  `short` varchar(4) NOT NULL,
  PRIMARY KEY(id),
  UNIQUE KEY `uc_disease_type_species_short` (`type`,`species`,`short`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- Table structure for table `iu_disease`
--

DROP TABLE IF EXISTS `iu_disease`;
CREATE TABLE `iu_disease` (
  `iu_id` bigint(20) NOT NULL,
  `disease_id` bigint(20) NOT NULL,
  PRIMARY KEY( iu_id, disease_id )
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- Table structure for table `iu_disease_group`
--

DROP TABLE IF EXISTS `iu_disease_group`;
CREATE TABLE `iu_disease_group` (
  `iu_id` bigint(20) NOT NULL,
  `disease_id` bigint(20) NOT NULL,
  `group_id` bigint(20) NOT NULL,
  PRIMARY KEY( iu_id, disease_id, group_id )
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2022-05-09 15:44:10


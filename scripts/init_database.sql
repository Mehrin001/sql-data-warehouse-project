/*
==========================================================================
Create Database and Schemas
==========================================================================
Script Purpose :
    This script creates a new databse named "Datawarehouse" after checking if it already exists.
    If the database exists, it is dropped and recreate. Additioanlly, the script sets up three schemas
    within the database : 'bronze', 'sliver' and 'gold'
WARNING:
    Running this script will drop the entire 'Datawarehouse' database if it exists.
    All data in the databse will be permanentlt deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

USE master;
 GO

 -- DROP AND RECREATE THE  "DATAWAREHOUSE" DATABASE
 IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
 BEGIN 
	 ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	 DROP DATABASE DataWarehouse;
 END;

 -- CREATE DATABASE  "DATAWAREHOUSE"
 GO
 CREATE DATABASE DataWarehouse;

 GO
 USE DataWarehouse;

 -- CREATING SCHEMA FOR EACH LAYER
 CREATE SCHEMA bronze;
 GO
 CREATE SCHEMA sliver;
 GO
 CREATE SCHEMA gold;

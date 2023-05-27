USE projects;

CREATE TABLE housing_data (
  UniqueID VARCHAR(20),
  ParcelID VARCHAR(20),
  LandUse VARCHAR(50),
  PropertyAddress VARCHAR(100),
  SaleDate VARCHAR(20),
  SalePrice INT,
  LegalReference VARCHAR(50),
  SoldAsVacant VARCHAR(5),
  OwnerName VARCHAR(100),
  OwnerAddress VARCHAR(100),
  Acreage FLOAT,
  TaxDistrict VARCHAR(50),
  LandValue FLOAT,
  BuildingValue FLOAT,
  TotalValue FLOAT,
  YearBuilt INT,
  Bedrooms INT,
  FullBath INT,
  HalfBath VARCHAR(10)
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Nashville Housing Data for Data Cleaning (reuploaded).csv'
INTO TABLE housing_data
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@UniqueID, @ParcelID, @LandUse, @PropertyAddress, @SaleDate, @SalePrice, @LegalReference, @SoldAsVacant, @OwnerName, @OwnerAddress, @Acreage, @TaxDistrict, @LandValue, @BuildingValue, @TotalValue, @YearBuilt, @Bedrooms, @FullBath, @HalfBath)
SET 
UniqueID = NULLIF(@UniqueID, ''),
ParcelID = NULLIF(@ParcelID, ''),
LandUse = NULLIF(@LandUse, ''),
PropertyAddress = NULLIF(@PropertyAddress, ''),
SaleDate = NULLIF(@SaleDate, ''),
SalePrice = NULLIF(@SalePrice, ''),
LegalReference = NULLIF(@LegalReference, ''),
SoldAsVacant = NULLIF(@SoldAsVacant, ''),
OwnerName = NULLIF(@OwnerName, ''),
OwnerAddress = NULLIF(@OwnerAddress, ''),
Acreage = NULLIF(@Acreage, ''),
TaxDistrict = NULLIF(@TaxDistrict, ''),
LandValue = NULLIF(@LandValue, ''),
BuildingValue = NULLIF(@BuildingValue, ''),
TotalValue = NULLIF(@TotalValue, ''),
YearBuilt = NULLIF(@YearBuilt, ''),
Bedrooms = NULLIF(@Bedrooms, ''),
FullBath = NULLIF(@FullBath, ''),
HalfBath = NULLIF(@HalfBath, '')
;

SELECT * 
FROM housing_data;

select count(*) 
FROM housing_data;

#CHECKING DATATYPES
DESCRIBE housing_data;

SELECT SaleDate,STR_TO_DATE(SaleDate, '%d-%b-%y') AS NewSaleDate
FROM housing_data;

ALTER TABLE housing_data
ADD NewSaleDate Date;

UPDATE housing_data
SET NewSaleDate = STR_TO_DATE(SaleDate, '%d-%b-%y');

SELECT *
FROM housing_data;
######################################################################################################

SELECT * FROM housing_data
WHERE PropertyAddress is NULL;
#CHECKING DUPLICATES
SELECT ParcelID, PropertyAddress, COUNT(*)
FROM housing_data
GROUP BY ParcelID, PropertyAddress
HAVING COUNT(*) > 1;

# THERE ARE DUPLICATES AND WE CHECK ONE CASE
SELECT ParcelID, PropertyAddress
FROM housing_data
WHERE ParcelID= "015 14 0 060.00";

SELECT * FROM housing_data
WHERE ParcelID is NULL;
# There are no Null values in the Parcel ID column

#Using Parcel ID we fill in the Null values in Propoert Address Column
SELECT a.ParcelID, a.PropertyAddress, b.ParcelID, b.PropertyAddress,IFNULL(a.PropertyAddress, b.PropertyAddress)
FROM housing_data a
JOIN housing_data b
	ON a.ParcelID = b.ParcelID
	AND a.UniqueID <> b.UniqueID
WHERE a.PropertyAddress IS NULL;

#updating the values
UPDATE housing_data a
JOIN housing_data b ON a.ParcelID = b.ParcelID AND a.UniqueID <> b.UniqueID
SET a.PropertyAddress = IFNULL(a.PropertyAddress, b.PropertyAddress)
WHERE a.PropertyAddress IS NULL;

SELECT * 
FROM housing_data;
##############################################################################################################################3

SELECT PropertyAddress,OwnerAddress
FROM housing_data;

SELECT 
SUBSTRING(PropertyAddress, 1, LOCATE(',',PropertyAddress)-1) as Address, 
SUBSTRING(PropertyAddress, LOCATE(',',PropertyAddress)+1, LENGTH(PropertyAddress)) as City
FROM housing_data;


ALTER TABLE housing_data
ADD PropertyAddressClean Nvarchar (250);

UPDATE housing_data
SET PropertyAddressClean = SUBSTRING(PropertyAddress, 1, LOCATE(',',PropertyAddress)-1);

ALTER TABLE housing_data
ADD Property_city Nvarchar (250);

UPDATE housing_data
SET Property_city = SUBSTRING(PropertyAddress, LOCATE(',',PropertyAddress)+1, LENGTH(PropertyAddress));

SELECT * 
FROM housing_data;

#We need to do the same wth the owner address
SELECT 
  SUBSTRING_INDEX(OwnerAddress, ',', 1) AS Address,
  TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(OwnerAddress, ',', 2), ',', -1)) AS City,
  TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(OwnerAddress, ',', 3), ',', -1)) AS State
FROM housing_data;

ALTER TABLE housing_data
ADD OwnerAddressClean NVARCHAR(250);

ALTER TABLE housing_data
ADD OwnerCity NVARCHAR(250);

ALTER TABLE housing_data
ADD OwnerState NVARCHAR(250);

UPDATE housing_data
SET OwnerAddressClean = SUBSTRING_INDEX(OwnerAddress, ',', 1);

UPDATE housing_data
SET OwnerCity = TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(OwnerAddress, ',', 2), ',', -1));

UPDATE housing_data
SET OwnerState = TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(OwnerAddress, ',', 3), ',', -1));

select * from housing_data;
###############################################################################################################3

SELECT Distinct(SoldAsVacant), COUNT(SoldAsVacant) 
FROM housing_data
Group by SoldAsVacant
ORDER BY 2;

SELECT SoldAsVacant,
CASE
	WHEN SoldAsVacant = 'Y' THEN 'Yes'
	WHEN SoldAsVacant = 'N' THEN 'No'
	ELSE SoldAsVacant
END
FROM housing_data;


UPDATE housing_data
SET SoldAsVacant=CASE
	WHEN SoldAsVacant = 'Y' THEN 'Yes'
	WHEN SoldAsVacant = 'N' THEN 'No'
	ELSE SoldAsVacant
END;
########################################################################################################

#Removing duplicates

with RowNumCTE as(
Select *,
	ROW_NUMBER () over (
	Partition by ParcelID,
				 PropertyAddress,
				 SalePrice,
				 SaleDate,
				 LegalReference
				 Order BY UniqueID
				 ) as row_num
From housing_data
)
Select*
FROM RowNumCTE
WHERE row_num>1;

#############Let's delete them
WITH RowNumCTE AS (
	SELECT *,
		ROW_NUMBER () over (
			PARTITION BY ParcelID,
				PropertyAddress,
				SalePrice,
				SaleDate,
				LegalReference
			ORDER BY UniqueID
		) as row_num
	FROM housing_data
)
DELETE FROM housing_data
WHERE UniqueID IN (
	SELECT UniqueID FROM RowNumCTE WHERE row_num > 1
);
######################################################################

ALTER TABLE housing_data
DROP COLUMN OwnerAddress,
DROP COLUMN PropertyAddress,
DROP COLUMN SaleDate;

select * from housing_data;

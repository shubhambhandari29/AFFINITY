IF OBJECT_ID(N'dbo.BranchMapping', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.BranchMapping (
        Email NVARCHAR(255) NOT NULL PRIMARY KEY,
        BranchName NVARCHAR(255) NOT NULL
    );
END;

WITH SeedData (Email, BranchName) AS (
    SELECT N'mdeluca@hanover.com', N'Northeast' UNION ALL
    SELECT N'jhoule@hanover.com', N'All' UNION ALL
    SELECT N'mbond@hanover.com', N'All' UNION ALL
    SELECT N'sh1bhandari@hanover.com', N'All' UNION ALL
    SELECT N'stscott@hanover.com', N'All' UNION ALL
    SELECT N'scarruth@hanover.com', N'All'
)
UPDATE target
SET BranchName = source.BranchName
FROM dbo.BranchMapping AS target
INNER JOIN SeedData AS source
    ON target.Email = source.Email;

WITH SeedData (Email, BranchName) AS (
    SELECT N'mdeluca@hanover.com', N'Northeast' UNION ALL
    SELECT N'jhoule@hanover.com', N'All' UNION ALL
    SELECT N'mbond@hanover.com', N'All' UNION ALL
    SELECT N'sh1bhandari@hanover.com', N'All' UNION ALL
    SELECT N'stscott@hanover.com', N'All' UNION ALL
    SELECT N'scarruth@hanover.com', N'All'
)
INSERT INTO dbo.BranchMapping (Email, BranchName)
SELECT source.Email, source.BranchName
FROM SeedData AS source
WHERE NOT EXISTS (
    SELECT 1
    FROM dbo.BranchMapping AS target
    WHERE target.Email = source.Email
);

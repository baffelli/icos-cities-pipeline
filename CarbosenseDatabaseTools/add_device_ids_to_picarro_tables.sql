CREATE TABLE IF NOT EXISTS `reference_instrument_deployment`
(
    `device_id` VARCHAR(32) NOT NULL;
    `date_from` TIMESTAMP NOT NULL;
    `date_to` TIMESTAMP NOT NULL;
    PRIMARY KEY(`device_id`, `date_from`, `date_to`) 
)

CREATE TABLE IF NOT EXISTS `reference_instrument`
(
    `device_id` VARCHAR(32) NOT NULL;
    `type` VARCHAR(64);
    `owner` VARCHAR(64);
    `comments` VARCHAR(64);
)

ALTER TABLE NABEL_DUE
    ADD COLUMN IF NOT EXISTS `device_id` VARCHAR(32) AFTER `timestamp`;

ALTER TABLE NABEL_PAY
    ADD COLUMN IF NOT EXISTS `device_id` VARCHAR(32) AFTER `timestamp`;

ALTER TABLE NABEL_RIG
    ADD COLUMN IF NOT EXISTS `device_id` VARCHAR(32) AFTER `timestamp`;

ALTER TABLE NABEL_HAE
    ADD COLUMN IF NOT EXISTS `device_id` VARCHAR(32) AFTER `timestamp`;
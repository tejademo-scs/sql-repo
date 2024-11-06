import { NextFunction, Request, response, Response } from "express";
import Sequelize from "sequelize";
import ApplicationHelper from "../helpers/ApplicationHelper";
import { fromString } from "uuidv4";
import CIRelationCisModel from "../models/CIRelationCisModel";
import sequelize from "./../db";
import CIIdentificationController from "./CIIdentificationDefController";
import CMDBDataSeedController from "./CMDBDataSeedController";
import CIClasscontroller from "./CIClassController";
import CMDBRelationshipsController from "./CMDBRelationshipsController";
import automation from "../utils/automation"
import moment from "moment";
import axios from "axios";
const auditLogsEnabled = process.env.AUDIT_LOGS_ENABLED
const actionDate = moment();

import * as fastcsv from "fast-csv";
import fs from "fs";
const https = require('https');

import logger from "../utils/logger";
import { cli } from "winston/lib/winston/config";
import UIConfigurationController from "./UIConfigurationController";
import getLocalString from "../utils/locale";
import auditLogsController from "./AuditLogsController";

const TICKETSERVICEINCIDENTURL = process.env.TICKET_SERVICE_INCIDENTS_URL;
const TICKETSERVICESRURL = process.env.TICKET_SERVICE_SR_URL;
const TICKETSERVICESCHANGEURL = process.env.TICKET_SERVICE_CHANGE_URL;

class CIsController extends ApplicationHelper {
  static getAllCIs = async (
    req: Request,
    res: Response,
    next: NextFunction
  ) => {
    try {
      const MIN_SIZE = 10;
      const { q, clientid, size = MIN_SIZE, page = 1, filter_attrs, order = 'DESC', column_name = 'last_discovered_time' } = req.body;
      const lang = req.headers.lang ? req.headers.lang : "en";
      const response: any = await getLocalString(lang);

      logger.info(`fetching All CIs for client id: ${clientid}`);
      if (size < 1 || page < 1)
        return next(new Error(response.info_pageSizeMustNotNegative));
      if (!Boolean(clientid)) return next(new Error(response.info_clientidRequired));

      const sortingQuery =
        Boolean(filter_attrs) && Boolean(Object.keys(filter_attrs).length)
          ? this.filterQuery(filter_attrs)
          : "";
      const columnNamesAs = (columnName: string[]) => {
        return columnName.map((cName: string) => {
          return `${cName} AS ${cName.replace(/con_cmdb_/, "")}`;
        });
      };
      const allColumns = [
        "con_cmdb_display_name",
        "con_cmdb_ci_category",
        "con_cmdb_source",
        "con_cmdb_unique_id"
      ];

      const getConfigurationitemColumn = await sequelize.query(
        `
                SELECT column_name FROM 
                information_schema.columns WHERE table_name = 'con_cmdb_configurationitem';
            `,
        {
          type: Sequelize.QueryTypes.SELECT,
        }
      );

      const CIsColumnName = columnNamesAs(
        getConfigurationitemColumn.map((clName: any) => clName.column_name)
      );
      const validColumns = CIsColumnName.filter(col => { if (/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(col.split(" ")[0])) return col });
      if (validColumns.length !== CIsColumnName.length) {
        return next(new Error('Invalid column names detected'));
      }
      const searchClause = (searchTerm: string) => {
        return {
          [Sequelize.Op.or]: allColumns.map((column) => ({
            [column]: {
              [Sequelize.Op.iLike]: `%${searchTerm}%`
            }
          }))
        };
      };
      const whereClause = {
        [Sequelize.Op.and]: [
          searchClause(q), // Call the search clause with the query term
          { con_cmdb_clientid: clientid }
        ]
      };
      const queryGenerator = (sequelize.getQueryInterface() as any).queryGenerator;
      const selectClause = q ?
        `SELECT ${CIsColumnName} FROM con_cmdb.con_cmdb_configurationitem
                    WHERE ${queryGenerator.getWhereConditions(whereClause)} ${sortingQuery}
                    ORDER BY con_cmdb_${column_name} ${order}
                    LIMIT :size 
                    OFFSET (:page - 1) * :size`
        : `SELECT ${CIsColumnName} FROM con_cmdb.con_cmdb_configurationitem
                    WHERE con_cmdb_clientid= :clientid ${sortingQuery}
                    ORDER BY con_cmdb_${column_name} ${order}
                    LIMIT :size 
                    OFFSET (:page - 1) * :size`;

      const replacements = {
        clientid: clientid,   // Placeholder for client_id
        size: size,             // Placeholder for size (limit)
        page: page              // Placeholder for page (pagination)
      };
      const items = await sequelize.query(selectClause, {
        replacements: replacements,
        type: Sequelize.QueryTypes.SELECT,
      });

      if (!Boolean(items.length)) {
        return res.status(200).send({
          message: response.info_dataNotFound,
          items,
          success: true,
        });
      }

      const selectCountClause = q
        ? `SELECT COUNT(*)  from con_cmdb.con_cmdb_configurationitem WHERE ${queryGenerator.getWhereConditions(whereClause)} ${sortingQuery}`
        : `SELECT COUNT(*)  from con_cmdb.con_cmdb_configurationitem WHERE con_cmdb_clientid= :clientid ${sortingQuery}`;

      const count = await sequelize.models.YourModel.count(, {
        replacements: { clientid: clientid },
        type: Sequelize.QueryTypes.SELECT,
      });

      const pages = Math.ceil(count / (size ? Number(size) : MIN_SIZE));

      return res.status(200).send({
        message: response.info_configuurationItemsFetchedSuccessfully,
        success: true,
        items,
        count,
        page: Number(page),
        pages,
      });
    } catch (error) {
      logger.error(new Error(`Error from getAllCIs ${error}`));
      next(error);
    }
  };

  static getCIsByType = async (
    req: Request,
    res: Response,
    next: NextFunction
  ) => {
    try {
      const MIN_SIZE = 10;
      const {
        q,
        clientid,
        ci_category,
        depth = 1,
        size = MIN_SIZE,
        page = 1,
        filter_attrs,
      } = req.body;
      const lang = req.headers.lang ? req.headers.lang : "en";
      const response: any = await getLocalString(lang);

      if (size < 1 || page < 1)
        return next(new Error(response.info_pageSizeMustNotNegative));
      if (!Boolean(clientid)) return next(new Error(response.info_clientidRequired));
      if (!Boolean(ci_category)) return next(new Error(response.info_ci_categoryRequired));
      const sortingQuery =
        Boolean(filter_attrs) && Boolean(Object.keys(filter_attrs).length)
          ? this.filterQuery(filter_attrs)
          : "";

      const columnNamesAs = (columnName: string[]) => {
        return columnName.map((cName: string) => {
          return `${cName} AS ${cName.replace(/con_cmdb_/, "")}`;
        });
      };
      const ciCategory = `con_cmdb_${ci_category}`;
      //error handling -class name not found message if table is not there
      logger.info("fetch CIs By Type ifrom db...");
      const getConfigurationitemColumn = await sequelize.query(
        `
                SELECT column_name FROM 
                information_schema.columns WHERE  table_name = '${ciCategory.toLowerCase()}';
            `,
        {
          type: Sequelize.QueryTypes.SELECT,
        }
      );
      if (!Boolean(getConfigurationitemColumn.length)) {
        return res.status(200).send({
          message: `${ciCategory} ${response.info_classDoesntFound}`,
          success: true,
        });
      }

      const CIsColumnName = columnNamesAs(
        getConfigurationitemColumn.map((clName: any) => clName.column_name)
      );
      const validColumns = CIsColumnName.filter(col => { if (/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(col.split(" ")[0])) return col });
      if (validColumns.length !== CIsColumnName.length) {
        return next(new Error('Invalid column names detected'));
      }
      const whereClause = {
        [Sequelize.Op.and]: [
          { con_cmdb_clientid: clientid },
          { con_cmdb_display_name: { [Sequelize.Op.iLike]: q } }
        ]
      }
      const queryGenerator = (sequelize.getQueryInterface() as any).queryGenerator;
      const selectClause = q
        ? `SELECT ${CIsColumnName} FROM con_cmdb.${ciCategory}
                    WHERE ${queryGenerator.getWhereConditions(whereClause)} ${sortingQuery}
                    ORDER BY con_cmdb_unique_id
                    LIMIT :size 
                    OFFSET (:page - 1) * :size`
        : `SELECT ${CIsColumnName} FROM con_cmdb.${ciCategory}
                    WHERE con_cmdb_clientid= :clientid ${sortingQuery}
                    ORDER BY con_cmdb_unique_id
                    LIMIT :size 
                    OFFSET (:page - 1) * :size`;

      const items = await sequelize.query(selectClause, {
        replacements: { size: size, page: page, clientid: clientid },
        type: Sequelize.QueryTypes.SELECT,
      });

      if (!Boolean(items.length)) {
        return res.status(200).send({
          message: response.info_dataNotFound,
          items,
          success: true,
        });
      }

      const selectCountClause = q
        ? `SELECT COUNT(*)  from con_cmdb.${ciCategory} WHERE ${queryGenerator.getWhereConditions(whereClause)} ${sortingQuery}`
        : `SELECT COUNT(*)  from con_cmdb.${ciCategory} WHERE con_cmdb_clientid= :clientid ${sortingQuery}`;

      const [{ count }] = <any>await sequelize.query(selectCountClause, {
        replacements: { clientid: clientid },
        type: Sequelize.QueryTypes.SELECT,
      });

      const pages = Math.ceil(count / (size ? Number(size) : MIN_SIZE));

      return res.status(200).send({
        message: response.info_configuurationItemsFetchedSuccessfully,
        success: true,
        items,
        count,
        page: Number(page),
        pages,
      });
    } catch (error) {
      logger.error(new Error(`Error from getAllCIs ${error}`));
      next(error);
    }
  };

  static getCIsByMultipleTypes = async (req: Request, res: Response, next: NextFunction) => {

    try {
      const MIN_SIZE = 10;
      const { q, clientid, depth = 1, size = MIN_SIZE, page = 1, filter_attrs = {}, order = 'DESC', column_name = 'last_discovered_time' } = req.body;
      const lang = req.headers.lang ? req.headers.lang : "en";
      const response: any = await getLocalString(lang);

      if (size < 1 || page < 1) return next(new Error(response.info_pageSizeMustNotNegative));
      if (!Boolean(clientid)) return next(new Error(response.info_clientidRequired));
      if ((!req.body.ci_category.length)) return next(new Error(response.info_ci_categoryRequired));
      const sortingQuery = (Boolean(filter_attrs) && Boolean(Object.keys(filter_attrs).length)) ? this.filterQuery(filter_attrs) : '';
      const columnNamesAs = (columnName: string[]) => {
        return columnName.map((cName: string) => {
          return `${cName} AS ${cName.replace(/con_cmdb_/, '')}`;
        });
      };


      logger.info(`fetch CIs By Multiple Types from db for client id : ${clientid}`);

      let ciCategoryValues = req.body.ci_category.length ? req.body.ci_category : [""]
      if ((filter_attrs.ci_category == "AWSEC2VMInstance" || filter_attrs.ci_category == "AzureVMInstance" || filter_attrs.ci_category == "GCPVMInstance" || filter_attrs.ci_category == "VMWareVMInstance" || filter_attrs.ci_category == "InstalledSoftware" || filter_attrs.ci_category == "ComputerSystem") && q !== null && q !== '' || (req.body.ci_category.includes("ComputerSystem") && q !== null && q !== '')) {
        const allColumns = ['con_cmdb_display_name', 'con_cmdb_ci_category', 'con_cmdb_source', 'con_cmdb_unique_id', 'con_cmdb_private_ip'];

        const searchClause = (q: string) => {
          return {
            [Sequelize.Op.or]: allColumns.map((column) => ({
              [column]: {
                [Sequelize.Op.iLike]: `%${q}%`
              }
            }))
          };
        };
        const getComputersystemColumn = await sequelize.query(`
        SELECT column_name FROM 
        information_schema.columns WHERE  table_name = 'con_cmdb_computersystem';
    `, {
          type: Sequelize.QueryTypes.SELECT
        });

        if (!Boolean(getComputersystemColumn.length)) {
          return res.status(200).send({
            message: response.info_configurationItemClassCouldntFind,
            success: true
          });
        }

        const CIsColumnName = columnNamesAs(getComputersystemColumn.map((clName: any) => clName.column_name));
        const validColumns = CIsColumnName.filter(col => { if (/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(col.split(" ")[0])) return col });
        if (validColumns.length !== CIsColumnName.length) {
          return next(new Error('Invalid column names detected'));
        }
        const whereClause = {
          [Sequelize.Op.and]: [
            { con_cmdb_clientid: clientid },
            Sequelize.where(
              Sequelize.fn('LOWER', Sequelize.col('con_cmdb_ci_category')),
              {
                [Sequelize.Op.in]: ciCategoryValues.map((val: string) => val.toLowerCase())
              }
            )
          ]
        };
        q ? whereClause[Sequelize.Op.and].push(searchClause(q) as any) : whereClause
        const queryGenerator = (sequelize.getQueryInterface() as any).queryGenerator;
        const items = await sequelize.query(`SELECT ${CIsColumnName} FROM con_cmdb.con_cmdb_computersystem
        WHERE ${queryGenerator.getWhereConditions(whereClause)} ${sortingQuery}
        ORDER BY con_cmdb_${column_name} ${order}
        LIMIT :size 
        OFFSET (:page - 1) * :size`
          , { replacements: { page: page, size: size }, type: Sequelize.QueryTypes.SELECT });

        if (!Boolean(items.length)) {
          return res.status(200).send({
            message: response.info_dataNotFound,
            items,
            success: true
          });
        };
        const selectCountClause =
          `SELECT COUNT(*)  from  con_cmdb.con_cmdb_computersystem WHERE ${queryGenerator.getWhereConditions(whereClause)} ${sortingQuery}`
        const [{ count }] = <any>await sequelize.query(selectCountClause, { replacements: { clientid: clientid }, type: Sequelize.QueryTypes.SELECT });
        const pages = Math.ceil(count / (size ? Number(size) : MIN_SIZE));
        return res.status(200).send({
          message: response.info_configuurationItemsFetchedSuccessfully,
          success: true,
          items,
          count,
          page: Number(page),
          pages
        });

      } else {
        const allColumns = ['con_cmdb_display_name', 'con_cmdb_ci_category', 'con_cmdb_source', 'con_cmdb_unique_id'];
        const searchClause = (q: string) => {
          return {
            [Sequelize.Op.or]: allColumns.map((column) => ({
              [column]: {
                [Sequelize.Op.iLike]: `%${q}%`
              }
            }))
          };
        };

        const getConfigurationitemColumn = await sequelize.query(`
        SELECT column_name FROM 
        information_schema.columns WHERE  table_name = 'con_cmdb_configurationitem';
    `, {
          type: Sequelize.QueryTypes.SELECT
        });

        if (!Boolean(getConfigurationitemColumn.length)) {
          return res.status(200).send({
            message: response.info_configurationItemClassCouldntFind,
            success: true
          });
        }

        const CIsColumnName = columnNamesAs(getConfigurationitemColumn.map((clName: any) => clName.column_name));
        const validColumns = CIsColumnName.filter(col => { if (/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(col.split(" ")[0])) return col });
        if (validColumns.length !== CIsColumnName.length) {
          return next(new Error('Invalid column names detected'));
        }
        const whereClause = {
          [Sequelize.Op.and]: [
            { con_cmdb_clientid: clientid },
            Sequelize.where(
              Sequelize.fn('LOWER', Sequelize.col('con_cmdb_ci_category')),
              {
                [Sequelize.Op.in]: ciCategoryValues.map((val: string) => val.toLowerCase())
              }
            )
          ]
        };
        q ? whereClause[Sequelize.Op.and].push(searchClause(q) as any) : whereClause
        const queryGenerator = (sequelize.getQueryInterface() as any).queryGenerator;
        const selectClause = q
          ? `SELECT ${CIsColumnName} FROM con_cmdb.con_cmdb_configurationitem
               WHERE ${queryGenerator.getWhereConditions(whereClause)}
               ${sortingQuery}
               ORDER BY con_cmdb_${column_name} ${order}
               LIMIT :size 
               OFFSET (:page - 1) * :size`
          : `SELECT ${CIsColumnName} FROM con_cmdb.con_cmdb_configurationitem
               WHERE ${queryGenerator.getWhereConditions(whereClause)} 
               ${sortingQuery}
               ORDER BY con_cmdb_${column_name} ${order}
               LIMIT :size 
               OFFSET (:page - 1) * :size`;

        const items = await sequelize.query(
          selectClause
          , {
            replacements: {
              clientid: clientid,
              size: size,
              page: page
            }, type: Sequelize.QueryTypes.SELECT
          });

        if (!Boolean(items.length)) {
          return res.status(200).send({
            message: response.info_dataNotFound,
            items,
            success: true
          });
        };
        const selectCountClause = q ?
          `SELECT COUNT(*)  from  con_cmdb.con_cmdb_configurationitem WHERE ${queryGenerator.getWhereConditions(whereClause)} ${sortingQuery}` :
          `SELECT COUNT(*)  from  con_cmdb.con_cmdb_configurationitem WHERE ${queryGenerator.getWhereConditions(whereClause)} ${sortingQuery}`;

        const [{ count }] = <any>await sequelize.query(selectCountClause, {
          replacements: {
            clientid: clientid,
          }, type: Sequelize.QueryTypes.SELECT
        });
        const pages = Math.ceil(count / (size ? Number(size) : MIN_SIZE));
        return res.status(200).send({
          message: response.info_configuurationItemsFetchedSuccessfully,
          success: true,
          items,
          count,
          page: Number(page),
          pages
        });
      }
    }
    catch (error) {
      logger.error(new Error(`Error from getAllCIs ${error}`));
      next(error);
    }
  };

  static moveChildCIsToManOrUnman = async (
    unique_id: string,
    clientid: string,
    isManaged: Boolean,
    response: any
  ) => {
    const relatedwhereClause = `parentci_id='${unique_id}' and clientid='${clientid}'`;

    const relatedcis: any = await sequelize.query(
      `
        SELECT * from con_cmdb.con_cmdb_relatedcis where ${relatedwhereClause}
    `,
      { type: Sequelize.QueryTypes.SELECT }
    );

    if (relatedcis.length > 0) {
      // for loop through i = 0 to length {
      for (let i = 0; i < relatedcis.length; i++) {
        const relatedciguid = relatedcis[i].childci_id;
        const relationship_name = relatedcis[i].relationship_name;
        const relationships: any = await sequelize.query(
          `select * from public.cmdb_relationships
              where relationship_name = '${relationship_name}' 
             `,
          { type: Sequelize.QueryTypes.SELECT }
        );
        if (!Boolean(relationships.length))
          return new Error(response.info_relationshipDoesNotExist);
        if (relationships[0]?.iscontained === true) {
          if (relatedciguid !== null && Boolean(relatedciguid.length)) {
            await sequelize.query(
              `UPDATE con_cmdb.con_cmdb_configurationitem
                  SET con_cmdb_ismanagedci = '${isManaged}'
                  WHERE con_cmdb_unique_id = '${relatedciguid}'`,
              { type: Sequelize.QueryTypes.UPDATE }
            );
          }
        }
      }

      const relatedwhereClause1 = `childci_id='${unique_id}' and clientid='${clientid}'`;

      const relatedcis1: any = await sequelize.query(
        `
        SELECT * from con_cmdb.con_cmdb_relatedcis where ${relatedwhereClause1}
     `,
        { type: Sequelize.QueryTypes.SELECT }
      );

      if (relatedcis1.length > 0) {
        //for loop through i = 0 to length {
        for (let i = 0; i < relatedcis1.length; i++) {
          const relatedciguid = relatedcis1[i].parentci_id;
          const relationship_name = relatedcis1[i].relationship_name;
          const relationships: any = await sequelize.query(
            `select * from public.cmdb_relationships
                  where relationship_name = '${relationship_name}' 
                 `,
            { type: Sequelize.QueryTypes.SELECT }
          );
          if (!Boolean(relationships.length))
            return new Error(response.info_relationshipDoesNotExist);
          if (relationships[0]?.iscontained === true) {
            if (relatedciguid !== null && Boolean(relatedciguid.length)) {
              await sequelize.query(
                `UPDATE con_cmdb.con_cmdb_configurationitem
                      SET con_cmdb_ismanagedci = '${isManaged}'
                      WHERE con_cmdb_unique_id = '${relatedciguid}'`,
                { type: Sequelize.QueryTypes.UPDATE }
              );
            }
          }
        }
      }
    }
  };

  static moveCIToManaged = async (
    req: Request,
    res: Response,
    next: NextFunction
  ) => {
    try {
      const { clientid, uuid } = req.body;
      logger.info(`moving CI To Managed for client id: ${clientid}`);
      const lang = req.headers.lang ? req.headers.lang : "en";
      const response: any = await getLocalString(lang);

      const getByUUID: any = await sequelize.query(
        `SELECT * FROM con_cmdb.con_cmdb_configurationitem WHERE con_cmdb_clientid= :clientid AND con_cmdb_unique_id= :uuid;
            `,
        { replacements: { clientid: clientid, uuid: uuid }, type: Sequelize.QueryTypes.SELECT }
      );

      if (!Boolean(getByUUID.length)) {
        return res.status(200).send({
          message: `${response.info_CIIsNotAvailableForTheGivenID} ${uuid}`,
          success: true,
        });
      }
      await sequelize.query(
        `UPDATE con_cmdb.con_cmdb_configurationitem
                  SET con_cmdb_ismanagedci = true
                  WHERE con_cmdb_unique_id = :uuid AND con_cmdb_clientid= :clientid`,
        { replacements: { clientid: clientid, uuid: uuid }, type: Sequelize.QueryTypes.UPDATE }
      );

      this.moveChildCIsToManOrUnman(uuid, clientid, true, response);

      return res.status(200).send({
        message: `CI with id ${uuid} has been moved to Managed CI space`,
        success: true,
      });
    } catch (error) {
      logger.error(new Error(`Error from movecitomanaged ${error}`));
      next(error);
    }
  };

  static moveCIToUnmanaged = async (
    req: Request,
    res: Response,
    next: NextFunction
  ) => {
    try {
      const { clientid, uuid } = req.body;
      logger.info(`moving CI To Unmanaged for client id: ${clientid}`);
      const lang = req.headers.lang ? req.headers.lang : "en";
      const response: any = await getLocalString(lang);
      const getByUUID: any = await sequelize.query(
        `SELECT * FROM con_cmdb.con_cmdb_configurationitem WHERE con_cmdb_clientid= :clientid AND con_cmdb_unique_id= :uuid;
            `,
        { replacements: { clientid: clientid, uuid: uuid }, type: Sequelize.QueryTypes.SELECT }
      );

      if (!Boolean(getByUUID.length)) {
        return res.status(200).send({
          message: `${response.info_CIIsNotAvailableForTheGivenID} ${uuid}`,
          success: true,
        });
      }
      await sequelize.query(
        `UPDATE con_cmdb.con_cmdb_configurationitem
                  SET con_cmdb_ismanagedci =false
                  WHERE con_cmdb_unique_id = :uuid AND con_cmdb_clientid= :clientid`,
        { replacements: { clientid: clientid, uuid: uuid }, type: Sequelize.QueryTypes.UPDATE }
      );

      this.moveChildCIsToManOrUnman(uuid, clientid, false, response);

      return res.status(200).send({
        message: `CI with id ${uuid} has been moved to Unmanaged CI space`,
        success: true,
      });
    } catch (error) {
      logger.error(new Error(`Error from getAllCIs ${error}`));
      next(error);
    }
  };

  static moveCIListToManaged = async (
    req: Request,
    res: Response,
    next: NextFunction
  ) => {
    try {
      const { uuids } = req.body;
      let { userid: userId }: any = req.headers;
      const lang = req.headers.lang ? req.headers.lang : "en";
      const response: any = await getLocalString(lang);
      const wrongUUID: any = [];
      let _updated_by = userId;
      let time = moment().format("yyyy-MM-DD HH:mm:ss");
      logger.info("moving CI List To Managed...");
      const attributesMap = await Promise.all(
        uuids.map(async (attr: any) => {
          const getCIs: any = await sequelize.query(
            `SELECT con_cmdb_clientid,con_cmdb_unique_id,con_cmdb_ci_category,con_cmdb_display_name,con_cmdb_cistatus,con_cmdb_created_date FROM con_cmdb.con_cmdb_configurationitem WHERE con_cmdb_clientid='${attr.clientid}' AND con_cmdb_unique_id='${attr.uuid}';`,
            { type: Sequelize.QueryTypes.SELECT }
          );

          if (!Boolean(getCIs.length)) {
            wrongUUID.push(attr.uuid);
          } else {
            getCIs[0].con_cmdb_old_managed_status = "unmanaged";
            getCIs[0].con_cmdb_new_managed_status = "managed";
            getCIs[0].con_cmdb_created_date = time
            getCIs[0].con_cmdb_updated_on = time
            getCIs[0].con_cmdb_updated_by = _updated_by;

            const keys = Object.keys(getCIs[0]).toString();
            const values = Object.values(getCIs[0]);
            const combinedValues = values.map((value: any) => {
              const findingString =
                typeof value !== "boolean" &&
                typeof value !== "number" &&
                typeof value !== "object" &&
                value !== null;
              return findingString ? `'${value}'` : value;
            });
            for (let index = 0; index < combinedValues.length; index++) {
              if (
                typeof combinedValues[index] == "object" &&
                combinedValues[index] !== null
              ) {
                combinedValues[index] =
                  "'" + JSON.stringify(combinedValues[index]) + "'";
              } else if (combinedValues[index] == null) {
                combinedValues[index] = "null";
              }
            }
            await sequelize.query(
              `UPDATE con_cmdb.con_cmdb_configurationitem
                      SET con_cmdb_ismanagedci = true
                      WHERE con_cmdb_unique_id = '${attr.uuid}' AND con_cmdb_clientid='${attr.clientid}'
                    `,
              { type: Sequelize.QueryTypes.UPDATE }
            );

            await sequelize.query(
              `INSERT INTO con_cmdb.con_cmdb_manageddevices_tracking (${keys}) VALUES (${combinedValues}) `,
              { type: Sequelize.QueryTypes.INSERT }
            );
          }
        })
      );

      return res.status(200).send({
        message: `${!wrongUUID.length
          ? response.info_CIHasBeenMovedToManagedCIList
          : `${response.info_CIHasBeenMovedToManagedCIListExcept} ${wrongUUID.join(
            ","
          )}`
          }`,
        success: true,
      });
    } catch (error) {
      logger.error(new Error(`Error from getAllCIs ${error}`));
      next(error);
    }
  };

  static moveCIListToUnmanaged = async (
    req: Request,
    res: Response,
    next: NextFunction
  ) => {
    try {
      const { uuids } = req.body;
      let { userid: userId }: any = req.headers;
      const lang = req.headers.lang ? req.headers.lang : "en";
      const response: any = await getLocalString(lang);
      const wrongUUID: any = [];
      let _updated_by = userId;
      let time = moment().format("yyyy-MM-DD HH:mm:ss");
      logger.info("moving CI List To Unmanaged...");
      const attributesMap = await Promise.all(
        uuids.map(async (attr: any) => {
          const getCIs: any = await sequelize.query(
            `SELECT con_cmdb_clientid,con_cmdb_unique_id,con_cmdb_ci_category,con_cmdb_display_name,con_cmdb_cistatus,con_cmdb_created_date FROM con_cmdb.con_cmdb_configurationitem WHERE con_cmdb_clientid='${attr.clientid}' AND con_cmdb_unique_id='${attr.uuid}';`,
            { type: Sequelize.QueryTypes.SELECT }
          );

          if (!Boolean(getCIs.length)) {
            wrongUUID.push(attr.uuid);
          } else {
            getCIs[0].con_cmdb_old_managed_status = "managed";
            getCIs[0].con_cmdb_new_managed_status = "unmanaged";
            getCIs[0].con_cmdb_created_date = time
            getCIs[0].con_cmdb_updated_on = time
            getCIs[0].con_cmdb_updated_by = _updated_by;
            const keys = Object.keys(getCIs[0]).toString();
            const values = Object.values(getCIs[0]);
            // const combinedValues = values.map((value: any) => {
            //   const findingString =
            //     typeof value !== "boolean" &&
            //     typeof value !== "number" &&
            //     typeof value !== "object" &&
            //     value !== null;
            //   return findingString ? `'${value}'` : value;
            // });
            // for (let index = 0; index < combinedValues.length; index++) {
            //   if (
            //     typeof combinedValues[index] == "object" &&
            //     combinedValues[index] !== null
            //   ) {
            //     combinedValues[index] =
            //       "'" + JSON.stringify(combinedValues[index]) + "'";
            //   } else if (combinedValues[index] == null) {
            //     combinedValues[index] = "null";
            //   }
            // }

            await sequelize.query(
              `UPDATE con_cmdb.con_cmdb_configurationitem
                  SET con_cmdb_ismanagedci = false
                  WHERE con_cmdb_unique_id = '${attr.uuid}' AND con_cmdb_clientid='${attr.clientid}'
                `,
              { type: Sequelize.QueryTypes.UPDATE }
            );
            const insertPlaceholders = values.map(() => "?").join(", ");
            await sequelize.query(
              `INSERT INTO con_cmdb.con_cmdb_manageddevices_tracking (${keys}) VALUES (${insertPlaceholders}) `,
              { replacements: values, type: Sequelize.QueryTypes.INSERT }
            );
          }
        })
      );

      return res.status(200).send({
        message: `${!wrongUUID.length
          ? response.info_CIHasBeenMovedToUNManagedCIList
          : `${response.info_CIHasBeenMovedToUNManagedCIListExcept} ${wrongUUID.join(
            ","
          )}`
          }`,
        success: true,
      });
    } catch (error) {
      logger.error(new Error(`Error from getAllCIs ${error}`));
      next(error);
    }
  };

  static getCIById = async (
    req: Request,
    res: Response,
    next: NextFunction
  ) => {
    try {
      const { unique_id, clientid, depth = 1 } = req.body;
      const lang = req.headers.lang ? req.headers.lang : "en";
      const response: any = await getLocalString(lang);
      if (!Boolean(clientid)) return next(new Error(response.info_clientidRequired));
      if (!Boolean(unique_id)) return next(new Error(response.info_uniqueIDRequired));
      logger.info(`fetch CI By Id from db for client id: ${clientid}`);
      const configurationItem = <any>await sequelize.query(
        `SELECT * from con_cmdb.con_cmdb_configurationitem WHERE con_cmdb_unique_id= :unique_id AND con_cmdb_clientid= :clientid
                `,
        { replacements: { clientid: clientid, unique_id: unique_id }, type: Sequelize.QueryTypes.SELECT }
      );

      const ciCategory = `con_cmdb_${configurationItem[0].con_cmdb_ci_category}`.toLowerCase();
      const isValidTableName = /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(ciCategory);
      if (!isValidTableName) return next(new Error('Table name is invalid, Please check once'))
      if (!Boolean(ciCategory)) return next(new Error(response.info_ci_categoryRequired));
     const ciCategoryTable = await sequelize.query(
    `SELECT * from con_cmdb."${ciCategory}" WHERE con_cmdb_unique_id= :unique_id AND con_cmdb_clientid= :clientid`,
    { replacements: { unique_id: unique_id, clientid: clientid }, type: Sequelize.QueryTypes.SELECT }
);

      const attributesMap = [ciCategoryTable].map((attr: any) => {
        const keys = Object.keys(attr[0]).map(
          (key) => `${key.replace(/con_cmdb_/, "")}`
        );
        const values = Object.values(attr[0]).map((val) => val);
        return {
          keys,
          values,
        };
      });
      const keys = attributesMap[0].keys;
      const values = attributesMap[0].values;

      const attrs = <any>{};
      for (let index = 0; index < keys.length; index++) {
        attrs[keys[index]] = values[index];
      }

      //child cis

      const childCIs = [];

      // const childCount=0

      if (depth > 1) {
        const relatedwhereClause = `parentci_id= :unique_id and clientid= :clientid`;

        const relatedcis: any = await sequelize.query(
          `
                    SELECT * from con_cmdb.con_cmdb_relatedcis where ${relatedwhereClause}
                `,
          { replacements: { clientid: clientid, unique_id: unique_id }, type: Sequelize.QueryTypes.SELECT }
        );

        if (relatedcis.length > 0) {
          let relationship_name;
          // for loop through i = 0 to length {
          for (let i = 0; i < relatedcis.length; i++) {
            const relatedciguid = relatedcis[i].childci_id;
            relationship_name = relatedcis[i].relationship_name;
            const relatedciCategory = `con_cmdb_${relatedcis[i].childci_classname}`.toLowerCase();
            const isValidTableName = /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(relatedciCategory);
            if (!isValidTableName) return next(new Error('Table name is invalid, Please check once'))
            const relatedci = <any>await sequelize.query(
              `SELECT * from con_cmdb."${relatedciCategory}" WHERE con_cmdb_unique_id= :relatedciguid AND con_cmdb_clientid= :clientid
                            `,
              { replacements: { relatedciguid: relatedciguid, clientid: clientid }, type: Sequelize.QueryTypes.SELECT }
            );
            if (relatedci.length > 0) {
              const relatedciattributesMap = [relatedci].map((attr: any) => {
                const keys = Object.keys(attr[0]).map(
                  (key) => `${key.replace(/con_cmdb_/, "")}`
                );
                const values = Object.values(attr[0]).map((val) => val);
                return {
                  keys,
                  values,
                };
              });

              const keys = relatedciattributesMap[0].keys;
              const values = relatedciattributesMap[0].values;
              const relatedattrs = <any>{};
              for (let index = 0; index < keys.length; index++) {
                relatedattrs[keys[index]] = values[index];
              }
              const childciitem = {
                citype: relatedattrs.ci_category,
                relationship_direction: "parent-to-child",
                relationship: relationship_name,
                attributes: relatedattrs,
              };

              childCIs.push(childciitem); //add childcitem to child cis list
            }
          }
        }

        const relatedwhereClause1 = `childci_id= :unique_id and clientid= :clientid`;

        const relatedcis1: any = await sequelize.query(
          `
                    SELECT * from con_cmdb.con_cmdb_relatedcis where ${relatedwhereClause1}
                `,
          { replacements: { unique_id: unique_id, clientid: clientid }, type: Sequelize.QueryTypes.SELECT }
        );

        if (relatedcis1.length > 0) {
          let relationship_name;
          //for loop through i = 0 to length {
          for (let i = 0; i < relatedcis1.length; i++) {
            const relatedciguid = relatedcis1[i].parentci_id;
            relationship_name = relatedcis1[i].relationship_name;
            const relatedciCategory = `con_cmdb_${relatedcis1[i].parentci_classname}`.toLowerCase();
            const isValidTableName = /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(relatedciCategory);
            if (!isValidTableName) return next(new Error('Table name is invalid, Please check once'))
            const relatedci = <any>await sequelize.query(
              `SELECT * from con_cmdb."${relatedciCategory}" WHERE con_cmdb_unique_id= :relatedciguid AND con_cmdb_clientid= :clientid
                            `,
              { replacements: { relatedciguid: relatedciguid, clientid: clientid }, type: Sequelize.QueryTypes.SELECT }
            );
            if (relatedci.length > 0) {
              const relatedciattributesMap = [relatedci].map((attr: any) => {
                const keys = Object.keys(attr[0]).map(
                  (key) => `${key.replace(/con_cmdb_/, "")}`
                );
                const values = Object.values(attr[0]).map((val) => val);
                return {
                  keys,
                  values,
                };
              });

              const keys = relatedciattributesMap[0].keys;
              const values = relatedciattributesMap[0].values;
              const relatedattrs = <any>{};
              for (let index = 0; index < keys.length; index++) {
                relatedattrs[keys[index]] = values[index];
              }
              const childciitem = {
                citype: relatedattrs.ci_category,
                relationship_direction: "child-to-parent",
                relationship: relationship_name,
                attributes: relatedattrs,
              };

              childCIs.push(childciitem); //add childcitem to child cis list
            }
          }
        }
      }

      const items = {
        citype: attrs.ci_category,
        clientid: attrs.clientid,
        attributes: attrs,
        childcis: childCIs,
      };
      // delete items.attributes.ci_category;
      // delete items.attributes.clientid;

      return res.status(200).send({
        message: response.info_configuurationItemsFetchedSuccessfully,
        success: true,
        items,
      });
    } catch (error) {
      logger.error(new Error(`Error from getCIById ${error}`));
      next(error);
    }
  };

  static getAllManagedCIs = async (
    req: Request,
    res: Response,
    next: NextFunction
  ) => {
    try {
      const MIN_SIZE = 30;
      const { q, clientid, size = MIN_SIZE, page = 1, filter_attrs } = req.body;
      const lang = req.headers.lang ? req.headers.lang : "en";
      const response: any = await getLocalString(lang);

      if (size < 1 || page < 1)
        return next(new Error(response.info_pageSizeMustNotNegative));
      if (!Boolean(clientid)) return next(new Error(response.info_clientidRequired));
      const sortingQuery =
        Boolean(filter_attrs) && Boolean(Object.keys(filter_attrs).length)
          ? this.filterQuery(filter_attrs)
          : "";

      const columnNamesAs = (columnName: string[]) => {
        return columnName.map((cName: string) => {
          return `${cName} AS ${cName.replace(/con_cmdb_/, "")}`;
        });
      };
      const ciCategory = `con_cmdb_configurationitem`;
      logger.info(`fetch All Managed CIs from db for client id: ${clientid}`);
      const getConfigurationitemColumn = await sequelize.query(
        `
                SELECT column_name FROM
                information_schema.columns WHERE table_name = '${ciCategory}';
            `,
        {
          type: Sequelize.QueryTypes.SELECT,
        }
      );

      const CIsColumnName = columnNamesAs(
        getConfigurationitemColumn.map((clName: any) => clName.column_name)
      );
      const validColumns = CIsColumnName.filter(col => { if (/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(col.split(" ")[0])) return col });
      if (validColumns.length !== CIsColumnName.length) {
        return next(new Error('Invalid column names detected'));
      }
      const whereClause = {
        [Sequelize.Op.and]: [
          { con_cmdb_clientid: clientid },
          { con_cmdb_display_name: { [Sequelize.Op.iLike]: q } }
        ]
      }
      const queryGenerator = (sequelize.getQueryInterface() as any).queryGenerator;
      const selectClause = q
        ? `SELECT ${CIsColumnName} FROM con_cmdb.${ciCategory}
                    WHERE ${queryGenerator.getWhereConditions(whereClause)} ${sortingQuery} AND con_cmdb_ismanagedci=true
                    ORDER BY con_cmdb_unique_id
                    LIMIT :size
                    OFFSET (:page - 1) * :size`
        : `SELECT ${CIsColumnName} FROM con_cmdb.${ciCategory}
                    WHERE con_cmdb_clientid= :clientid ${sortingQuery} AND con_cmdb_ismanagedci=true
                    ORDER BY con_cmdb_unique_id
                    LIMIT :size
                    OFFSET (:page - 1) * :size`;
      const items = await sequelize.query(selectClause, {
        replacements: { clientid: clientid, page: page, size: size },
        type: Sequelize.QueryTypes.SELECT,
      });

      if (!Boolean(items.length)) {
        return res.status(200).send({
          message: response.info_dataNotFound,
          items,
          success: true,
        });
      }

      const selectCountClause = q
        ? `SELECT COUNT(*)  from con_cmdb.${ciCategory} WHERE ${queryGenerator.getWhereConditions(whereClause)} ${sortingQuery} AND con_cmdb_ismanagedci=true`
        : `SELECT COUNT(*)  from con_cmdb.${ciCategory} WHERE con_cmdb_clientid= :clientid ${sortingQuery} AND con_cmdb_ismanagedci=true`;

      const [{ count }] = <any>await sequelize.query(selectCountClause, {
        replacements: { clientid: clientid },
        type: Sequelize.QueryTypes.SELECT,
      });
      const pages = Math.ceil(count / (size ? Number(size) : MIN_SIZE));
      return res.status(200).send({
        message: response.info_configuurationItemsFetchedSuccessfully,
        success: true,
        items,
        count,
        page: Number(page),
        pages,
      });
    } catch (error) {
      logger.error(new Error(`Error from getAllCIs ${error}`));
      next(error);
    }
  };

  static getManagedCIsByType = async (
    req: Request,
    res: Response,
    next: NextFunction
  ) => {
    try {
      const MIN_SIZE = 10;
      const {
        q,
        clientid,
        ci_category,
        depth = 1,
        size = MIN_SIZE,
        page = 1,
        filter_attrs,
        citype,
      } = req.body;
      const lang = req.headers.lang ? req.headers.lang : "en";
      const response: any = await getLocalString(lang);

      if (size < 1 || page < 1)
        return next(new Error(response.info_pageSizeMustNotNegative));
      if (!Boolean(clientid)) return next(new Error(response.info_clientidRequired));
      if (!Boolean(ci_category)) return next(new Error(response.info_ci_categoryRequired));
      const sortingQuery =
        Boolean(filter_attrs) && Boolean(Object.keys(filter_attrs).length)
          ? this.filterQuery(filter_attrs)
          : "";

      const columnNamesAs = (columnName: string[]) => {
        return columnName.map((cName: string) => {
          return `${cName} AS ${cName.replace(/con_cmdb_/, "")}`;
        });
      };
      const ciCategory = `con_cmdb_${ci_category}`;
      logger.info(`fetch Managed CIs By Type from db for client id: ${clientid}`);
      const getConfigurationitemColumn = await sequelize.query(
        `
                SELECT column_name FROM 
                information_schema.columns WHERE table_name = :ciCategoryValue;
            `,
        {
          replacements: { ciCategoryValue: ciCategory.toLowerCase() }, type: Sequelize.QueryTypes.SELECT,
        }
      );
      const CIsColumnName = columnNamesAs(
        getConfigurationitemColumn.map((clName: any) => clName.column_name)
      );
      const ciCategoryValues = ci_category?.toLowerCase();
      const query1 = `con_cmdb.${ciCategory?.toLowerCase()}
            WHERE  con_cmdb_clientid= :clientid AND con_cmdb_ismanagedci=true`;
      const query2 = `con_cmdb.con_cmdb_${citype?.toLowerCase()}
            WHERE LOWER(con_cmdb_ci_category)= :ciCategoryValues AND con_cmdb_clientid= :clientid AND con_cmdb_ismanagedci=true`;
      const ci_type = citype ? query2 : query1;
      const searchTerm = `%${q}%`
      const selectClause = q
        ? `SELECT ${CIsColumnName} FROM ${ci_type} AND con_cmdb_display_name ILIKE :searchTerm ${sortingQuery}
                    ORDER BY con_cmdb_unique_id
                    LIMIT :size 
                    OFFSET (:page - 1) * :size`
        : `SELECT ${CIsColumnName} FROM ${ci_type} ${sortingQuery}
                    ORDER BY con_cmdb_unique_id
                    LIMIT :size 
                    OFFSET (:page - 1) * :size`;

      const items = await sequelize.query(selectClause, {
        replacements: { clientid: clientid, ciCategoryValues: ciCategoryValues, page: page, size: size, searchTerm: searchTerm }, type: Sequelize.QueryTypes.SELECT,
      });

      if (!Boolean(items.length)) {
        return res.status(200).send({
          message: response.info_dataNotFound,
          items,
          success: true,
        });
      }

      const selectCountClause = q
        ? `SELECT COUNT(*)  from ${ci_type} AND con_cmdb_display_name ILIKE :searchTerm ${sortingQuery}`
        : `SELECT COUNT(*)  from ${ci_type} ${sortingQuery}`;

      const [{ count }] = <any>await sequelize.query(selectCountClause, {
        replacements: { clientid: clientid, ciCategoryValues: ciCategoryValues, searchTerm: searchTerm },
        type: Sequelize.QueryTypes.SELECT,
      });

      const pages = Math.ceil(count / (size ? Number(size) : MIN_SIZE));

      return res.status(200).send({
        message: response.info_configuurationItemsFetchedSuccessfully,
        success: true,
        items,
        count,
        page: Number(page),
        pages,
      });
    } catch (error) {
      logger.error(new Error(`Error from getAllCIs ${error}`));
      next(error);
    }
  };

  static getManagedCIById = async (
    req: Request,
    res: Response,
    next: NextFunction
  ) => {
    try {
      const { unique_id, clientid, depth = 1 } = req.body;
      const lang = req.headers.lang ? req.headers.lang : "en";
      const response: any = await getLocalString(lang);

      if (!Boolean(clientid)) return next(new Error(response.info_clientidRequired));
      if (!Boolean(unique_id)) return next(new Error(response.info_uniqueIDRequired));
      logger.info(`fetch Managed CI By Id from db for client id: ${clientid}`);
      const configurationItem = <any>await sequelize.query(
        `SELECT * from con_cmdb.con_cmdb_configurationitem WHERE con_cmdb_unique_id= :unique_id AND con_cmdb_clientid= :clientid AND con_cmdb_ismanagedci=true
                `,
        { replacements: { unique_id: unique_id, clientid: clientid }, type: Sequelize.QueryTypes.SELECT }
      );

      if (!configurationItem.length)
        return next(
          new Error(response.info_CINotFoundForUniqueID)
        );

      const ciCategory = `con_cmdb_${configurationItem[0].con_cmdb_ci_category}`;
      // if (!db_table_names.includes(ciCategory.toLowerCase())) {
      //   return next(new Error('Dbname not exists in the database'))
      // }
      const ciCategoryTable = <any>await sequelize.query(
        `SELECT * from con_cmdb."${ciCategory.toLowerCase()}" WHERE con_cmdb_unique_id= :unique_id AND con_cmdb_clientid= :clientid AND con_cmdb_ismanagedci=true
                `,
        { replacements: { unique_id: unique_id, clientid: clientid }, type: Sequelize.QueryTypes.SELECT }
      );

      if (!ciCategoryTable.length)
        return next(
          new Error(
            `Data Not Found From ${ciCategory.replace(/con_cmdb_/, "")}`
          )
        );

      const attributesMap = [ciCategoryTable].map((attr: any) => {
        const keys = Object.keys(attr[0]).map(
          (key) => `${key.replace(/con_cmdb_/, "")}`
        );
        const values = Object.values(attr[0]).map((val) => val);
        return {
          keys,
          values,
        };
      });
      const keys = attributesMap[0].keys;
      const values = attributesMap[0].values;

      const attrs = <any>{};
      for (let index = 0; index < keys.length; index++) {
        attrs[keys[index]] = values[index];
      }

      const items = {
        citype: attrs.ci_category,
        clientid: attrs.clientid,
        attributes: attrs,
      };
      delete items.attributes.ci_category;
      delete items.attributes.clientid;

      return res.status(200).send({
        message: response.info_configuurationItemsFetchedSuccessfully,
        success: true,
        items,
      });
    } catch (error) {
      logger.error(new Error(`Error from getCIById ${error}`));
      next(error);
    }
  };

  static getAllUnManagedCIs = async (
    req: Request,
    res: Response,
    next: NextFunction
  ) => {
    try {
      const { q, clientid } = req.body;
      const lang = req.headers.lang ? req.headers.lang : "en";
      const response: any = await getLocalString(lang);

      if (!Boolean(clientid)) return next(new Error(response.info_clientidRequired));

      const columnNamesAs = (columnName: string[]) => {
        return columnName.map((cName: string) => {
          return `${cName} AS ${cName.replace(/con_cmdb_/, "")}`;
        });
      };
      const ciCategory = `con_cmdb_configurationitem`;
      logger.info(`fetching All Un Managed CIs for client id: ${clientid}`);
      const getConfigurationitemColumn = await sequelize.query(
        `
                SELECT column_name FROM 
                information_schema.columns WHERE table_name = '${ciCategory}';
            `,
        {
          type: Sequelize.QueryTypes.SELECT,
        }
      );

      const CIsColumnName = columnNamesAs(
        getConfigurationitemColumn.map((clName: any) => clName.column_name)
      );
      const searchTerm = `%${q}%`
      const selectClause = q
        ? `SELECT ${CIsColumnName} FROM con_cmdb.${ciCategory}
                    WHERE con_cmdb_display_name ILIKE :searchTerm AND con_cmdb_clientid= :clientid AND con_cmdb_ismanagedci=false
                    ORDER BY con_cmdb_unique_id`
        : `SELECT ${CIsColumnName} FROM con_cmdb.${ciCategory}
                    WHERE con_cmdb_clientid= :clientid AND con_cmdb_ismanagedci=false
                    ORDER BY con_cmdb_unique_id`;

      const items = await sequelize.query(selectClause, {
        replacements: { clientid: clientid, searchTerm: searchTerm }, type: Sequelize.QueryTypes.SELECT,
      });

      if (!Boolean(items.length)) {
        return res.status(200).send({
          message: response.info_dataNotFound,
          items,
          success: true,
        });
      }

      const selectCountClause = q
        ? `SELECT COUNT(*)  from con_cmdb.${ciCategory} WHERE con_cmdb_display_name ILIKE :searchTerm AND con_cmdb_clientid= :clientid AND con_cmdb_ismanagedci=false`
        : `SELECT COUNT(*)  from con_cmdb.${ciCategory} WHERE con_cmdb_clientid= :clientid AND con_cmdb_ismanagedci=false`;

      const [{ count }] = <any>await sequelize.query(selectCountClause, {
        replacements: { clientid: clientid, searchTerm: searchTerm }, type: Sequelize.QueryTypes.SELECT,
      });

      return res.status(200).send({
        message: response.info_configuurationItemsFetchedSuccessfully,
        success: true,
        items,
        count,
      });
    } catch (error) {
      logger.error(new Error(`Error from getAllCIs ${error}`));
      next(error);
    }
  };

  static getUnManagedCIsByType = async (
    req: Request,
    res: Response,
    next: NextFunction
  ) => {
    try {
      const MIN_SIZE = 10;
      const {
        q,
        clientid,
        ci_category,
        depth = 1,
        size = MIN_SIZE,
        page = 1,
        filter_attrs,
        citype,
      } = req.body;
      const lang = req.headers.lang ? req.headers.lang : "en";
      const response: any = await getLocalString(lang);

      if (size < 1 || page < 1)
        return next(new Error(response.info_pageSizeMustNotNegative));
      if (!Boolean(clientid)) return next(new Error(response.info_clientidRequired));
      if (!Boolean(ci_category)) return next(new Error(response.info_ci_categoryRequired));
      const sortingQuery =
        Boolean(filter_attrs) && Boolean(Object.keys(filter_attrs).length)
          ? this.filterQuery(filter_attrs)
          : "";

      const columnNamesAs = (columnName: string[]) => {
        return columnName.map((cName: string) => {
          return `${cName} AS ${cName.replace(/con_cmdb_/, "")}`;
        });
      };
      const ciCategory = `con_cmdb_${ci_category}`;
      logger.info(`fetching UnManaged CIs By Type for client id: ${clientid}`);
      const getConfigurationitemColumn = await sequelize.query(
        `
                SELECT column_name FROM 
                information_schema.columns WHERE table_name = :ciCategoryValue;
            `,
        {
          replacements: { ciCategoryValue: ciCategory.toLowerCase() }, type: Sequelize.QueryTypes.SELECT,
        }
      );
      const CIsColumnName = columnNamesAs(
        getConfigurationitemColumn.map((clName: any) => clName.column_name)
      );

      if (CIsColumnName.length < 1) {
        return res.status(200).send({
          message: response.info_tableNotFound,
        });
      }
      const query1 = `con_cmdb.${ciCategory?.toLowerCase()}
            WHERE  con_cmdb_clientid= :clientid AND con_cmdb_ismanagedci=false`;
      const query2 = `con_cmdb.con_cmdb_${citype?.toLowerCase()}
            WHERE LOWER(con_cmdb_ci_category)= :ci_category_value AND con_cmdb_clientid= :clientid AND con_cmdb_ismanagedci=false`;
      const ci_type = citype ? query2 : query1;

      const selectClause = q
        ? `SELECT ${CIsColumnName} FROM ${ci_type} AND con_cmdb_display_name ILIKE :search ${sortingQuery}
                    ORDER BY con_cmdb_unique_id
                    LIMIT :size 
                    OFFSET (:page - 1) * :size`
        : `SELECT ${CIsColumnName} FROM ${ci_type} ${sortingQuery}
                    ORDER BY con_cmdb_unique_id
                    LIMIT :size 
                    OFFSET (:page - 1) * :size`;

      const items = await sequelize.query(selectClause, {
        replacements: { search: `%${q}%`, page: page, size: size, clientid: clientid, ci_category_value: ci_category?.toLowerCase() }, type: Sequelize.QueryTypes.SELECT,
      });

      if (!Boolean(items.length)) {
        return res.status(200).send({
          message: response.info_dataNotFound,
          items,
          success: true,
        });
      }

      const selectCountClause = q
        ? `SELECT COUNT(*)  from ${ci_type} AND con_cmdb_display_name ILIKE :search ${sortingQuery}`
        : `SELECT COUNT(*)  from ${ci_type} ${sortingQuery}`;

      const [{ count }] = <any>await sequelize.query(selectCountClause, {
        replacements: { search: `%${q}%`, clientid: clientid, ci_category_value: ci_category?.toLowerCase() }, type: Sequelize.QueryTypes.SELECT,
      });

      const pages = Math.ceil(count / (size ? Number(size) : MIN_SIZE));

      return res.status(200).send({
        message: response.info_configuurationItemsFetchedSuccessfully,
        success: true,
        items,
        count,
        page: Number(page),
        pages,
      });
    } catch (error) {
      logger.error(new Error(`Error from getAllCIs ${error}`));
      next(error);
    }
  };

  static getUnManagedCIById = async (
    req: Request,
    res: Response,
    next: NextFunction
  ) => {
    try {
      const { unique_id, clientid, depth = 1 } = req.body;
      const lang = req.headers.lang ? req.headers.lang : "en";
      const response: any = await getLocalString(lang);

      if (!Boolean(clientid)) return next(new Error(response.info_clientidRequired));
      if (!Boolean(unique_id)) return next(new Error(response.info_uniqueIDRequired));
      logger.info(`fetching UnManaged CI By Id for client id: ${clientid}`);
      const configurationItem = <any>await sequelize.query(
        `SELECT * from con_cmdb.con_cmdb_configurationitem WHERE con_cmdb_unique_id= :unique_id AND con_cmdb_clientid= :clientid AND con_cmdb_ismanagedci=false
                `,
        { replacements: { unique_id: unique_id, clientid: clientid }, type: Sequelize.QueryTypes.SELECT }
      );

      if (!configurationItem.length)
        return next(new Error(response.info_dataNotFoundFromConfigurationItem));

      const ciCategory = `con_cmdb_${configurationItem[0].con_cmdb_ci_category.toLowerCase()}`;
      const ciCategoryTable = <any>await sequelize.query(
        `SELECT * from con_cmdb."${ciCategory}" WHERE con_cmdb_unique_id= :unique_id AND con_cmdb_clientid= :clientid AND con_cmdb_ismanagedci=false
                `,
        { replacements: { unique_id: unique_id, clientid: clientid }, type: Sequelize.QueryTypes.SELECT }
      );

      if (!ciCategoryTable.length)
        return next(
          new Error(
            `Data Not Found From ${ciCategory.replace(/con_cmdb_/, "")}`
          )
        );

      const attributesMap = [ciCategoryTable].map((attr: any) => {
        const keys = Object.keys(attr[0]).map(
          (key) => `${key.replace(/con_cmdb_/, "")}`
        );
        const values = Object.values(attr[0]).map((val) => val);
        return {
          keys,
          values,
        };
      });
      const keys = attributesMap[0].keys;
      const values = attributesMap[0].values;

      const attrs = <any>{};
      for (let index = 0; index < keys.length; index++) {
        attrs[keys[index]] = values[index];
      }

      const items = {
        citype: attrs.ci_category,
        clientid: attrs.clientid,
        attributes: attrs,
      };
      delete items.attributes.ci_category;
      delete items.attributes.clientid;

      return res.status(200).send({
        message: response.info_configuurationItemsFetchedSuccessfully,
        success: true,
        items,
      });
    } catch (error) {
      logger.error(new Error(`Error from getCIById ${error}`));
      next(error);
    }
  };

  static createOrUpdateCI = async (
    citype: string,
    attributes: any,
    clientId: string,
    identificationDef: any
  ) => {
    const attr_exclusion_list = [
      "con_cmdb_source",
      "con_cmdb_created_by",
      "con_cmdb_description",
      "con_cmdb_display_name",
      "con_cmdb_unique_id",
      "con_cmdb_label",
      "con_cmdb_asset_id",
      "con_cmdb_last_modified_by",
      "con_cmdb_asset_tag",
      "con_cmdb_last_modified_time",
      "con_cmdb_ci_category",
      "con_cmdb_installed_date",
      "con_cmdb_created_date",
      "con_cmdb_ci_role",
      "con_cmdb_config_last_modified_time",
      "con_cmdb_contact_details",
      "con_cmdb_last_audit_status",
      "con_cmdb_last_audit_time",
      "con_cmdb_state_time",
      "con_cmdb_state",
      "con_cmdb_clientid",
      "con_cmdb_ci_owner",
      "con_cmdb_cinum",
      "con_cmdb_clientname",
      "con_cmdb_ci_subcategory",
      "con_cmdb_location",
      "con_cmdb_ismanagedci",
      "con_cmdb_last_discovered_time",
      "con_cmdb_discovery_runidentifier",
    ];

    logger.info(`attributes before =>> ${attributes}`);

    let ciUUID = <any>"";
    const ciIdentificationRules = identificationDef.ciidentificationrules;
    ciIdentificationRules.sort(
      (a: any, b: any) => Number(a.priority) - Number(b.priority)
    );
    const ciTableName = `con_cmdb_${citype}`;
    const ct_ciTableName = `ct_${ciTableName}`;
    let ciExists = false;
    const curr_timestamp = moment().format("yyyy-MM-DD HH:mm:ss");
    // if (!Boolean(attributes.created_date)) {
    //     attributes.created_date = curr_timestamp
    // }

    // if (!Boolean(attributes.last_modified_time)) {
    //     attributes.last_modified_time = curr_timestamp
    // }

    if (!Boolean(attributes.last_discovered_time)) {
      attributes.last_discovered_time = curr_timestamp;
    }
    const runid = attributes.discovery_runidentifier;

    if (!Boolean(attributes.cistatus)) {
      attributes.cistatus = "active";
    }

    if (!Boolean(attributes.ci_category)) {
      attributes.ci_category = citype;
    }

    let enable_default_tracking = false;
    const records = <any>(
      await sequelize.query(
        `select * from public.cmdb_properties where client_id ='${clientId}' `,
        { type: Sequelize.QueryTypes.SELECT }
      )
    );
    if (records.length == 1) {
      if (records[0].enable_default_changetracking == true) {
        logger.info("entered 2nd loop");
        enable_default_tracking = true;
      }
    }

    logger.info(`attributes after =>> ${attributes}`);

    for (let index = 0; index < ciIdentificationRules.length; index++) {
      const identificationRule = ciIdentificationRules[index];
      logger.info(`Processing Identification Rules => ${identificationRule}`);
      const { criterion_attributes, allownull, priority } = identificationRule;
      const criterionAttributeList = criterion_attributes.split(",");
      logger.info(`criterionAttributeList =>>> ${criterionAttributeList}`);
      let whereClause = `con_cmdb_clientid='${clientId}'`;

      if (!allownull) {
        let atleastOneAttrNull = false;
        for (
          let attrIndex = 0;
          attrIndex < criterionAttributeList.length;
          attrIndex++
        ) {
          const attr = criterionAttributeList[attrIndex];
          const CIPayloadAttr = attributes[attr];
          if (CIPayloadAttr !== undefined) {
            logger.info("Attribute defined ");
            if (CIPayloadAttr === "" || CIPayloadAttr === null) {
              atleastOneAttrNull = true;
              break;
            } else {
              whereClause += ` and con_cmdb_${attr}='${attributes[attr]}'`;
            }
          } else {
            atleastOneAttrNull = true;
            break;
          }
        }
        attributes.last_modified_time = curr_timestamp;
        //  attributes.last_discovered_time = curr_timestamp

        if (!atleastOneAttrNull) {
          //rule matching
          logger.info("Identification Rules Matched...");
          const CIFromDB = <any>(
            await sequelize.query(
              `SELECT * FROM con_cmdb.${ciTableName.toLowerCase()} WHERE ${whereClause};`,
              { type: Sequelize.QueryTypes.SELECT }
            )
          );
          if ((await CIFromDB).length > 0) {
            ciExists = true;
            logger.info("Updating existing CI");
            const Cisfromdb = [CIFromDB].map((attr) => {
              const keys = Object.keys(attr[0]).map((key) => key);
              const values = Object.values(attr[0]).map((val) => val);
              return {
                keys,
                values,
              };
            });
            const keys = Cisfromdb[0].keys;
            const values = Cisfromdb[0].values;

            const attributesMap: any = [attributes].map((attr: any) => {
              // attr['last_modified_time'] = moment().format('yyyy-MM-DD HH:mm:ss');
              const keys = Object.keys(attr).map((key) => `con_cmdb_${key}`);
              const values = Object.values(attr).map((val) => val);
              return {
                keys,
                values,
              };
            })[0];

            const combinedValues = attributesMap.values.map(
              (value: any, indx: any) => {
                const findingString =
                  typeof value !== "boolean" &&
                  typeof value !== "number" &&
                  typeof value !== "object" &&
                  value !== null;
                if (typeof value == "object") {
                  if (value == null) {
                    return `${attributesMap.keys[indx]}= null `;
                  } else {
                    return `${attributesMap.keys[indx]}= ${"'" + JSON.stringify(value) + "'"
                      }`;
                  }
                } else {
                  return findingString
                    ? `${attributesMap.keys[indx]}='${value}'`
                    : `${attributesMap.keys[indx]}=${value}`;
                }
              }
            );
            // const combinedValues1 = values.map((value: any) => {
            //     const findingString =
            //         typeof value !== 'boolean' &&
            //         typeof value !== 'number' &&
            //         typeof value !== 'object' &&
            //         value !== null;
            //     return findingString ? `'${value}'` : value;
            // });
            // for (let index = 0; index < combinedValues1.length; index++) {
            //     if (typeof combinedValues1[index] == 'object' && combinedValues1[index] !== null) {

            //         combinedValues1[index] = "'" + combinedValues1[index].toISOString() + "'"

            //     } else if (combinedValues1[index] == null) {
            //         combinedValues1[index] = "null"

            //     }

            // }

            let updateCI = false;
            let updateCI_ct = false;
            for (let i = 0; i < keys.length; i++) {
              for (let index = 0; index < attributesMap.keys.length; index++) {
                if (keys[i] == attributesMap.keys[index]) {
                  if (
                    keys[i] != "con_cmdb_last_modified_time" &&
                    keys[i] != "con_cmdb_created_date" &&
                    keys[i] != "con_cmdb_last_discovered_time" &&
                    keys[i] != "con_cmdb_discovery_runidentifier" &&
                    keys[i] != "con_cmdb_installed_date"
                  ) {
                    if (values[i] !== attributesMap.values[index]) {
                      logger.info(`updated values--------->>>>> ${keys[i]}`);
                      logger.info(
                        `${values[i]} ========== ${attributesMap.values[index]}`
                      );
                      updateCI = true;
                    }
                  }
                  if (keys[i] == attributesMap.keys[index]) {
                    if (
                      attr_exclusion_list.indexOf(attributesMap.keys[index]) ===
                      -1
                    ) {
                      if (values[i] !== attributesMap.values[index])
                        updateCI_ct = true;
                    }
                  }
                }
              }
            }
            if (!updateCI == true) {
              if (runid !== null) {
                await sequelize.query(
                  `UPDATE con_cmdb.${ciTableName.toLowerCase()} SET con_cmdb_last_discovered_time ='${attributes.last_discovered_time
                  }',con_cmdb_discovery_runidentifier='${runid}' WHERE ${whereClause}`,
                  { type: Sequelize.QueryTypes.UPDATE }
                );

                logger.info(
                  "No change in cis so only discovered time and run identifier updated"
                );
              } else {
                await sequelize.query(
                  `UPDATE con_cmdb.${ciTableName.toLowerCase()} SET con_cmdb_last_discovered_time ='${attributes.last_discovered_time
                  }'WHERE ${whereClause}`,
                  { type: Sequelize.QueryTypes.UPDATE }
                );
                logger.info("No change in cis so only discovered time updated");
              }
              ciUUID = (await CIFromDB)[0]["con_cmdb_unique_id"];
              break;
            } else {
              await sequelize.query(
                `UPDATE con_cmdb.${ciTableName.toLowerCase()} SET ${combinedValues} WHERE ${whereClause}`,
                { type: Sequelize.QueryTypes.UPDATE }
              );
              ciUUID = (await CIFromDB)[0]["con_cmdb_unique_id"];
              logger.info(`ciUUID while updating existing CI : ${ciUUID}`);
            }
            const updated_CIFromDB: any = await sequelize.query(
              `select * from con_cmdb.${ciTableName.toLowerCase()} WHERE ${whereClause}`,
              { type: Sequelize.QueryTypes.SELECT }
            );
            if (updateCI_ct == true) {
              let ci_baselines = await sequelize.query(
                `select baseline_name, max_level from public.ci_baseline where clientid = '${clientId}' and citype = '${citype}' and is_enabled = true`,
                { type: Sequelize.QueryTypes.SELECT }
              );
              logger.info(`ci_baseline : ${ci_baselines}`);
              //latest edited
              if (enable_default_tracking) {
                ci_baselines.push({ baseline_name: "default", max_level: 10 });
              }

              ci_baselines.forEach(async (baseline: any) => {
                const baseline_name = baseline.baseline_name;
                const max_level = baseline.max_level;
                const ct_created = curr_timestamp;

                let ct_insertwhereClause = `${whereClause} AND con_cmdb_baseline_name ='${baseline_name}' AND con_cmdb_ci_operation = 'insert'`;
                const ct_CIFromDB_insert = await sequelize.query(
                  `select * from con_cmdb.${ct_ciTableName} where ${ct_insertwhereClause}`,
                  { type: Sequelize.QueryTypes.SELECT }
                );

                if (ct_CIFromDB_insert.length == 0) {
                  CIFromDB[0].con_cmdb_baseline_name = baseline_name;
                  CIFromDB[0].con_cmdb_ci_operation = "insert";
                  CIFromDB[0].con_cmdb_ct_created = ct_created;
                  const keys = Object.keys(CIFromDB[0]).toString();
                  const values = Object.values(CIFromDB[0]);

                  const combinedValues = values.map((value: any) => {
                    const findingString =
                      typeof value !== "boolean" &&
                      typeof value !== "number" &&
                      typeof value !== "object" &&
                      value !== null;
                    return findingString ? `'${value}'` : value;
                  });
                  for (let index = 0; index < combinedValues.length; index++) {
                    if (
                      typeof combinedValues[index] == "object" &&
                      combinedValues[index] !== null
                    ) {
                      combinedValues[index] =
                        "'" + JSON.stringify(combinedValues[index]) + "'";
                    } else if (combinedValues[index] == null) {
                      combinedValues[index] = "null";
                    }
                  }
                  await sequelize.query(
                    `INSERT INTO con_cmdb.${ct_ciTableName.toLowerCase()} (${keys}) VALUES (${combinedValues}) `,
                    { type: Sequelize.QueryTypes.INSERT }
                  );
                } //-----------------for older cis baseline inserted and updated

                let ct_updatewhereClause = `${whereClause} and con_cmdb_baseline_name='${baseline_name}' and con_cmdb_ci_operation='update'`;
                const ct_CIFromDB_udpate = await sequelize.query(
                  `select * from con_cmdb.${ct_ciTableName} where ${ct_updatewhereClause}`,
                  { type: Sequelize.QueryTypes.SELECT }
                );
                if (ct_CIFromDB_udpate.length >= max_level) {
                  let fetch_limit = ct_CIFromDB_udpate.length - max_level + 1;
                  await sequelize.query(
                    `delete from con_cmdb.${ct_ciTableName.toLowerCase()} where ${ct_updatewhereClause} AND con_cmdb_ct_created IN (select con_cmdb_ct_created from con_cmdb.${ct_ciTableName.toLowerCase()} where ${ct_updatewhereClause} order by
                                         con_cmdb_ct_created FETCH NEXT ${fetch_limit} ROWS ONLY) `,
                    { type: Sequelize.QueryTypes.DELETE }
                  );
                }

                updated_CIFromDB[0].con_cmdb_baseline_name = baseline_name;
                updated_CIFromDB[0].con_cmdb_ci_operation = "update";
                updated_CIFromDB[0].con_cmdb_ct_created = ct_created;

                const keys1 = Object.keys(updated_CIFromDB[0]).toString();
                const values1 = Object.values(updated_CIFromDB[0]);

                const combinedValues1 = values1.map((value: any) => {
                  const findingString =
                    typeof value !== "boolean" &&
                    typeof value !== "number" &&
                    typeof value !== "object" &&
                    value !== null;
                  return findingString ? `'${value}'` : value;
                });
                for (let index = 0; index < combinedValues1.length; index++) {
                  if (
                    typeof combinedValues1[index] == "object" &&
                    combinedValues1[index] !== null
                  ) {
                    combinedValues1[index] =
                      "'" + JSON.stringify(combinedValues1[index]) + "'";
                  } else if (combinedValues1[index] == null) {
                    combinedValues1[index] = "null";
                  }
                }
                await sequelize.query(
                  `INSERT INTO con_cmdb.${ct_ciTableName.toLowerCase()} (${keys1}) VALUES (${combinedValues1}) `,
                  { type: Sequelize.QueryTypes.INSERT }
                );
              });
            }
            break;
          }

          break;
        }
      } else {
        //Nulls are allowed
        let atleastOneAttrExist = false;
        logger.info(
          `${criterionAttributeList} -> Nulls are allowed criterionAttributeList`
        );
        for (
          let attrIndex = 0;
          attrIndex < criterionAttributeList.length;
          attrIndex++
        ) {
          const attr = criterionAttributeList[attrIndex];
          const CIPayloadAttr = attributes[attr];
          logger.info(`Processing attributes : ${attr}`);
          if (CIPayloadAttr !== undefined) {
            logger.info("Attribute defined");
            if (CIPayloadAttr !== null) {
              if (CIPayloadAttr.length > 0) {
                atleastOneAttrExist = true;
                whereClause += ` and con_cmdb_${attr}='${attributes[attr]}'`;
              }
            }
          }
        }
        if (atleastOneAttrExist) {
          //rule matching
          logger.info("Identification Rule Matched(Null values allowed)");
          const CIFromDB: any = sequelize.query(
            `SELECT * FROM con_cmdb.${ciTableName.toLowerCase()} WHERE ${whereClause};`,
            { type: Sequelize.QueryTypes.SELECT }
          );
          logger.info("Updating existing CI");
          if ((await CIFromDB).length > 0) {
            ciExists = true;

            logger.info("Updating existing CI");
            const Cisfromdb = [CIFromDB].map((attr) => {
              const keys = Object.keys(attr[0]).map((key) => key);
              const values = Object.values(attr[0]).map((val) => val);
              return {
                keys,
                values,
              };
            });
            const keys = Cisfromdb[0].keys;
            const values = Cisfromdb[0].values;
            //--------------------------------------
            const attributesMap = [attributes].map((attr: any) => {
              attr["last_modified_time"] = moment().format(
                "yyyy-MM-DD HH:mm:ss"
              );
              const keys = Object.keys(attr).map((key) => `con_cmdb_${key}`);
              const values = Object.values(attr).map((val) => val);
              return {
                keys,
                values,
              };
            })[0];
            // const combinedValues = attributesMap.values.map(value => {
            //     const findingString =
            //         typeof value !== 'boolean' &&
            //         typeof value !== 'number' &&
            //         value !== null;
            //     return findingString ? `'${value}'` : `${value}`;
            // });
            const combinedValues = attributesMap.values.map((value, indx) => {
              // const findingNonString =
              //     typeof value !== 'boolean' &&
              //     typeof value !== 'number';
              // return `${attributesMap.keys[indx]}='${value}'`
              const findingString =
                typeof value !== "boolean" &&
                typeof value !== "number" &&
                typeof value !== "object" &&
                value !== null;
              if (typeof value == "object") {
                if (value == null) {
                  return `${attributesMap.keys[indx]}= null `;
                } else {
                  return `${attributesMap.keys[indx]}= ${"'" + JSON.stringify(value) + "'"
                    }`;
                }
              } else {
                return findingString
                  ? `${attributesMap.keys[indx]}='${value}'`
                  : `${attributesMap.keys[indx]}=${value}`;
              }
            });
            let updateCI = false;
            let updateCI_ct = false;
            for (let i = 0; i < keys.length; i++) {
              for (let index = 0; index < attributesMap.keys.length; index++) {
                if (keys[i] == attributesMap.keys[index]) {
                  if (
                    keys[i] != "con_cmdb_last_modified_time" &&
                    keys[i] != "con_cmdb_created_date" &&
                    keys[i] != "con_cmdb_last_discovered_time" &&
                    keys[i] != "con_cmdb_discovery_runidentifier" &&
                    keys[i] != "con_cmdb_installed_date"
                  ) {
                    if (values[i] !== attributesMap.values[index]) {
                      updateCI = true;
                    }
                  }
                  if (keys[i] == attributesMap.keys[index]) {
                    if (
                      attr_exclusion_list.indexOf(attributesMap.keys[index]) ===
                      -1
                    ) {
                      if (values[i] !== attributesMap.values[index])
                        updateCI_ct = true;
                    }
                  }
                }
              }
            }
            if (!updateCI == true) {
              await sequelize.query(
                `UPDATE con_cmdb.${ciTableName.toLowerCase()} SET con_cmdb_last_discovered_time ='${curr_timestamp}' WHERE ${whereClause}`,
                { type: Sequelize.QueryTypes.UPDATE }
              );

              ciUUID = (await CIFromDB)[0]["con_cmdb_unique_id"];
              break;
            } else {
              await sequelize.query(
                `UPDATE con_cmdb.${ciTableName.toLowerCase()} SET ${combinedValues} WHERE ${whereClause}`,
                { type: Sequelize.QueryTypes.UPDATE }
              );
              ciUUID = (await CIFromDB)[0]["con_cmdb_unique_id"];
              logger.info(`ciUUID while updating existing CI : ${ciUUID}`);
            }
            const updated_CIFromDB: any = await sequelize.query(
              `select * from con_cmdb.${ciTableName.toLowerCase()} WHERE ${whereClause}`,
              { type: Sequelize.QueryTypes.SELECT }
            );
            if (updateCI_ct == true) {
              let ci_baselines = await sequelize.query(
                `select baseline_name, max_level from public.ci_baseline where clientid = '${clientId}' and citype = '${citype}' and is_enabled = true`,
                { type: Sequelize.QueryTypes.SELECT }
              );
              logger.info(`ci_baseline ::::::::: ${ci_baselines}`);
              if (enable_default_tracking) {
                ci_baselines.push({ baseline_name: "default", max_level: 10 });
              }

              ci_baselines.forEach(async (baseline: any) => {
                const baseline_name = baseline.baseline_name;
                const max_level = baseline.max_level;
                const ct_created = curr_timestamp;

                let ct_insertwhereClause = `${whereClause} AND con_cmdb_baseline_name ='${baseline_name}' AND con_cmdb_ci_operation = 'insert'`;
                const ct_CIFromDB_insert = await sequelize.query(
                  `select * from con_cmdb.${ct_ciTableName} where ${ct_insertwhereClause}`,
                  { type: Sequelize.QueryTypes.SELECT }
                );

                if (ct_CIFromDB_insert.length == 0) {
                  CIFromDB[0].con_cmdb_baseline_name = baseline_name;
                  CIFromDB[0].con_cmdb_ci_operation = "insert";
                  CIFromDB[0].con_cmdb_ct_created = ct_created;

                  const keys = Object.keys(CIFromDB[0]).toString();
                  const values = Object.values(CIFromDB[0]);
                  const combinedValues = values.map((value: any) => {
                    const findingString =
                      typeof value !== "boolean" &&
                      typeof value !== "number" &&
                      typeof value !== "object" &&
                      value !== null;
                    return findingString ? `'${value}'` : value;
                  });

                  for (let index = 0; index < combinedValues.length; index++) {
                    if (
                      typeof combinedValues[index] == "object" &&
                      combinedValues[index] !== null
                    ) {
                      combinedValues[index] =
                        "'" + JSON.stringify(combinedValues[index]) + "'";
                    } else if (combinedValues[index] == null) {
                      combinedValues[index] = "null";
                    }
                  }
                  await sequelize.query(
                    `INSERT INTO con_cmdb.${ct_ciTableName.toLowerCase()} (${keys}) VALUES (${combinedValues}) `,
                    { type: Sequelize.QueryTypes.INSERT }
                  );
                  //-----------------for older cis baseline inserted and updated

                  let ct_updatewhereClause = `${whereClause} and con_cmdb_baseline_name='${baseline_name}' and con_cmdb_ci_operation='update'`;
                  const ct_CIFromDB_udpate = await sequelize.query(
                    `select * from con_cmdb.${ct_ciTableName} where ${ct_updatewhereClause}`,
                    { type: Sequelize.QueryTypes.SELECT }
                  );
                  if (ct_CIFromDB_udpate.length >= max_level) {
                    let fetch_limit = ct_CIFromDB_udpate.length - max_level + 1;
                    await sequelize.query(
                      `delete from con_cmdb.${ct_ciTableName.toLowerCase()} where ${ct_updatewhereClause} AND con_cmdb_ct_created IN (select con_cmdb_ct_created from con_cmdb.${ct_ciTableName.toLowerCase()} where ${ct_updatewhereClause} order by
                                             con_cmdb_ct_created FETCH NEXT ${fetch_limit} ROWS ONLY) `,
                      { type: Sequelize.QueryTypes.DELETE }
                    );
                  }
                  updated_CIFromDB[0].con_cmdb_baseline_name = baseline_name;
                  updated_CIFromDB[0].con_cmdb_ci_operation = "update";
                  updated_CIFromDB[0].con_cmdb_ct_created = ct_created;

                  const keys1 = Object.keys(updated_CIFromDB[0]).toString();
                  const values1 = Object.values(updated_CIFromDB[0]);

                  const combinedValues1 = values1.map((value: any) => {
                    const findingString =
                      typeof value !== "boolean" &&
                      typeof value !== "number" &&
                      typeof value !== "object" &&
                      value !== null;
                    return findingString ? `'${value}'` : value;
                  });
                  for (let index = 0; index < combinedValues1.length; index++) {
                    if (
                      typeof combinedValues1[index] == "object" &&
                      combinedValues1[index] !== null
                    ) {
                      combinedValues1[index] =
                        "'" + JSON.stringify(combinedValues1[index]) + "'";
                    } else if (combinedValues1[index] == null) {
                      combinedValues1[index] = "null";
                    }
                  }
                  await sequelize.query(
                    `INSERT INTO con_cmdb.${ct_ciTableName.toLowerCase()} (${keys1}) VALUES (${combinedValues}) `,
                    { type: Sequelize.QueryTypes.INSERT }
                  );
                }
              });
            }
            break;
          }
        }
      }
    }

    //New CI record

    if (!ciExists) {
      attributes.created_date = curr_timestamp;
      attributes.last_modified_time = curr_timestamp;
      for (let index = 0; index < ciIdentificationRules.length; index++) {
        const defaultCIIdentificationRule = ciIdentificationRules[index];

        const { criterion_attributes, allownull } = defaultCIIdentificationRule;

        const criterionAttributeList =
          criterion_attributes && criterion_attributes.split(",");

        let UUIDString = clientId + citype;

        logger.info(
          `${criterionAttributeList} -> criterionAttributeList new CI record`
        );
        if (!allownull) {
          //nulls not allowed
          let atleastOneAttrNull = false;
          for (
            let attrIndex = 0;
            attrIndex < criterionAttributeList.length;
            attrIndex++
          ) {
            const attr = criterionAttributeList[attrIndex];
            const CIPayloadAttr = attributes[attr];

            logger.info(`Processing attributes : ${attr}`);
            if (CIPayloadAttr !== undefined) {
              logger.info("Attribute defined");
              if (CIPayloadAttr === "" || CIPayloadAttr === null) {
                atleastOneAttrNull = true;
                break;
              } else {
                //  UUIDString = UUIDString + CIPayloadAttr + clientId;
                UUIDString = UUIDString + CIPayloadAttr;
              }
            } else {
              logger.info(`Attributes not provided : ${attr}`);
              atleastOneAttrNull = true;
            }
          }
          if (!atleastOneAttrNull) {
            //rule matching
            logger.info("Identification Rule Matched");
            const UUID = fromString(UUIDString);
            const isManageCI = false;

            let attributesMap = [attributes].map((attr: any) => {
              const keys = Object.keys(attr).map((key) => `con_cmdb_${key}`);
              keys.push("con_cmdb_unique_id");
              const values = Object.values(attr).map((val) => val);
              values.push(UUID);
              if (attributes.ismanagedci === undefined) {
                keys.push("con_cmdb_ismanagedci");
                values.push(isManageCI);
              }
              return {
                keys,
                values,
              };
            })[0];

            // Insert CI into database
            logger.info("Inserting new CI into the database");
            //=======================>
            const combinedValues = attributesMap.values.map((value) => {
              const findingString =
                typeof value !== "boolean" &&
                typeof value !== "number" &&
                typeof value !== "object" &&
                value !== null;
              return findingString ? `'${value}'` : value;
            });
            for (let index = 0; index < combinedValues.length; index++) {
              if (
                typeof combinedValues[index] == "object" &&
                combinedValues[index] !== null
              ) {
                combinedValues[index] =
                  "'" + JSON.stringify(combinedValues[index]) + "'";
              } else if (combinedValues[index] == null) {
                combinedValues[index] = "null";
              }
            }
            await sequelize.query(
              `INSERT INTO con_cmdb.${ciTableName.toLowerCase()} (${attributesMap.keys
              }) VALUES (${combinedValues})`,
              { type: Sequelize.QueryTypes.INSERT }
            );
            ciUUID = UUID;
            let ci_baselines = await sequelize.query(
              `select baseline_name, max_level from public.ci_baseline where clientid = '${clientId}' and citype = '${citype}' and is_enabled = true`,
              { type: Sequelize.QueryTypes.SELECT }
            );
            if (enable_default_tracking) {
              ci_baselines.push({ baseline_name: "default", max_level: 10 });
            }

            ci_baselines.forEach(async (baseline: any) => {
              const baseline_name = baseline.baseline_name;
              const ci_operation = "insert";
              const ct_created = curr_timestamp;
              logger.info(
                `output expected::::::  ${baseline_name}, ${ci_operation}, ${ct_created}`
              );

              attributesMap = [attributes].map((attr: any) => {
                const keys = Object.keys(attr).map((key) => `con_cmdb_${key}`);
                keys.push("con_cmdb_baseline_name");
                keys.push("con_cmdb_ci_operation");
                keys.push("con_cmdb_ct_created");
                keys.push("con_cmdb_unique_id");
                const values = Object.values(attr).map((val) => val);
                values.push(baseline_name);
                values.push(ci_operation);
                values.push(ct_created);
                values.push(UUID);

                logger.info(`<<<< ${keys} = ${values}`);
                return {
                  keys,
                  values,
                };
              })[0];
              const combinedValues = attributesMap.values.map((value: any) => {
                const findingString =
                  typeof value !== "boolean" &&
                  typeof value !== "number" &&
                  typeof value !== "object" &&
                  value !== null;
                return findingString ? `'${value}'` : value;
              });
              for (let index = 0; index < combinedValues.length; index++) {
                if (
                  typeof combinedValues[index] == "object" &&
                  combinedValues[index] !== null
                ) {
                  combinedValues[index] =
                    "'" + JSON.stringify(combinedValues[index]) + "'";
                } else if (combinedValues[index] == null) {
                  combinedValues[index] = "null";
                }
              }
              await sequelize.query(
                `INSERT INTO con_cmdb.${ct_ciTableName.toLowerCase()} (${attributesMap.keys
                }) VALUES (${combinedValues})`,
                { type: Sequelize.QueryTypes.INSERT }
              );

              logger.info(`${combinedValues} >>>>>>combined values`);
            });
            break;
          }
        } else {
          // nulls are allowed
          logger.info("nulls are allowed part 2");

          let atleastOneAttrExist = false;
          logger.info(
            `${criterionAttributeList} ->criterionAttributeList nulls are allowed`
          );
          for (
            let attrIndex = 0;
            attrIndex < criterionAttributeList.length;
            attrIndex++
          ) {
            const attr = criterionAttributeList[attrIndex];
            const CIPayloadAttr = attributes[attr];
            logger.info(`Processing attributes : ${attr}`);
            if (CIPayloadAttr !== undefined) {
              logger.info("Attribute defined");
              if (CIPayloadAttr !== null) {
                if (CIPayloadAttr.length > 0) {
                  atleastOneAttrExist = true;
                  // UUIDString = UUIDString + CIPayloadAttr + clientId;
                  UUIDString = UUIDString + CIPayloadAttr;
                }
              }
            }
          }
          if (atleastOneAttrExist) {
            //rule matching
            logger.info("Identification Rule Matched (Null values allowed)");
            const UUID = fromString(UUIDString);
            const isManageCI = false;

            let attributesMap = [attributes].map((attr: any) => {
              //=======================>
              attr["created_date"] = moment().format("yyyy-MM-DD hh:mm:ss");
              attr["last_modified_time"] = attr["created_date"];
              const keys = Object.keys(attr).map((key) => `con_cmdb_${key}`);
              keys.push("con_cmdb_unique_id");
              const values = Object.values(attr).map((val) => val);
              values.push(UUID);
              if (attributes.ismanagedci === undefined) {
                keys.push("con_cmdb_ismanagedci");
                values.push(isManageCI);
              }
              return {
                keys,
                values,
              };
            })[0];

            // Insert CI into database
            logger.info("Inserting new CI into the database");
            const combinedValues = attributesMap.values.map((value) => {
              const findingNonString =
                typeof value !== "boolean" &&
                typeof value !== "number" &&
                typeof value !== "object" &&
                value !== null;
              return findingNonString ? `'${value}'` : value;
            });
            for (let index = 0; index < combinedValues.length; index++) {
              if (
                typeof combinedValues[index] == "object" &&
                combinedValues[index] !== null
              ) {
                combinedValues[index] =
                  "'" + JSON.stringify(combinedValues[index]) + "'";
              } else if (combinedValues[index] == null) {
                combinedValues[index] = "null";
              }
            }
            await sequelize.query(
              `INSERT INTO con_cmdb.${ciTableName.toLowerCase()} (${attributesMap.keys
              }) VALUES (${combinedValues})`,
              { type: Sequelize.QueryTypes.INSERT }
            );
            ciUUID = UUID;
            let ci_baselines = await sequelize.query(
              `select baseline_name, max_level from public.ci_baseline where clientid = '${clientId}' and citype = '${citype}' and is_enabled = true`,
              { type: Sequelize.QueryTypes.SELECT }
            );
            logger.info(`ci_baseline :::::::: ${ci_baselines}`);
            if (enable_default_tracking) {
              ci_baselines.push({ baseline_name: "default", max_level: 10 });
            }

            ci_baselines.forEach(async (baseline: any) => {
              const baseline_name = baseline.baseline_name;
              const ci_operation = "insert";
              const ct_created = curr_timestamp;
              logger.info(
                `output expected:::::: ${baseline_name}, ${ci_operation}, ${ct_created}`
              );

              attributesMap = [attributes].map((attr: any) => {
                const keys = Object.keys(attr).map((key) => `con_cmdb_${key}`);
                keys.push("con_cmdb_baseline_name");
                keys.push("con_cmdb_ci_operation");
                keys.push("con_cmdb_ct_created");
                keys.push("con_cmdb_unique_id");
                const values = Object.values(attr).map((val) => val);
                values.push(baseline_name);
                values.push(ci_operation);
                values.push(ct_created);
                values.push(UUID);

                logger.info(`<<<< ${keys} = ${values}`);
                return {
                  keys,
                  values,
                };
              })[0];
              const combinedValues = attributesMap.values.map((value: any) => {
                const findingString =
                  typeof value !== "boolean" &&
                  typeof value !== "number" &&
                  typeof value !== "object" &&
                  value !== null;
                return findingString ? `'${value}'` : value;
              });
              for (let index = 0; index < combinedValues.length; index++) {
                if (
                  typeof combinedValues[index] == "object" &&
                  combinedValues[index] !== null
                ) {
                  combinedValues[index] =
                    "'" + JSON.stringify(combinedValues[index]) + "'";
                } else if (combinedValues[index] == null) {
                  combinedValues[index] = "null";
                }
              }
              await sequelize.query(
                `INSERT INTO con_cmdb.${ct_ciTableName.toLowerCase()} (${attributesMap.keys
                }) VALUES (${combinedValues})`,
                { type: Sequelize.QueryTypes.INSERT }
              );

              logger.info(`${combinedValues} >>>>>>combined values`);
            });
            break;
          }
        }
      }
    }
    return { ciUUID, ciExists };
  };

  static getCiRelationCis = async (req: Request, res: Response, next: NextFunction) => {
    try {
      const MIN_SIZE = 10;
      const {
        q,
        sortByColumn,
        sortByOrder,
        size = MIN_SIZE,
        page = 1,
      } = req.query as any;
      const { clientid }: any = req.headers;
      const lang = req.headers.lang ? req.headers.lang : "en";
      const response: any = await getLocalString(lang);

      if (size < 1 || page < 1)
        return new Error(response.info_pageSizeMustNotNegative);
      if (!Boolean(clientid)) return new Error(response.info_clientidRequired);

      let where = {};
      if (Boolean(q)) {
        const columns = [
          "childci_classname",
          "parentci_classname",
          "parentci_id",
          "relationship_name",
        ];
        where = await this.searchQuery(columns, q);
      }
      logger.info("fetch Ci Relation Cis from db...");
      const { count, rows: items } = await CIRelationCisModel.findAndCountAll({
        order: [
          [
            sortByColumn ? sortByColumn : "parentci_id",
            sortByOrder ? sortByOrder : "ASC",
          ],
        ],
        limit: size ? size : MIN_SIZE,
        offset: page ? size * (page - 1) : 0,
        where: { clientid: clientid, ...where },
      });
      if (items.length <= 0) {
        return res.status(200).send({
          message: response.info_dataNotFound,
          items,
          success: true,
        });
      }
      if (!Boolean(items.length)) return new Error(response.info_dataNotFound);

      const pages = Math.ceil(count / (size ? Number(size) : MIN_SIZE));

      return res.status(200).send({
        message: response.info_dataFetchedSuccessfully,
        success: true,
        items,
        count,
        page: Number(page),
        pages,
      });
    } catch (error: any) {
      logger.error(new Error(error));
      next(error);
      // return res.status(500).send({ error: error });
    }
  };

  static getCiRelationCisById = async (req: Request, res: Response, next: NextFunction) => {
    try {
      const { id } = req.params;
      const lang = req.headers.lang ? req.headers.lang : "en";
      const response: any = await getLocalString(lang);

      logger.info("fetch Ci Relation Cis By Id from db...");
      const items = await CIRelationCisModel.findAll({
        where: { relatedci_id: id },
      });
      return res.status(200).send({
        message: response.info_fetchedSuccessfully,
        success: true,
        items,
      });
    } catch (error: any) {
      logger.error(new Error(error));
      next(error);
    }
  };

  static createOrUpdateRelationship = async (
    parentCIUUID: any,
    parentClassType: any,
    relationshipname: any,
    childCIUUID: string,
    childClassType: any,
    clientId: string,
    created_by: string
  ) => {
    //check the relatedCIs table with where clause parentciid=parentCIUUID and relationshipname=relationshipname and childciid=childCIUUID
    if (
      Boolean(parentCIUUID) &&
      Boolean(parentClassType) &&
      Boolean(relationshipname) &&
      Boolean(childCIUUID) &&
      Boolean(childClassType) &&
      Boolean(clientId)
    ) {
      const whereClause = `parentci_id='${parentCIUUID}' and relationship_name='${relationshipname}' and childci_id='${childCIUUID}'`;

      const relatedcis = await sequelize.query(
        `
            SELECT * from con_cmdb.con_cmdb_relatedcis where ${whereClause}
            `,
        { type: Sequelize.QueryTypes.SELECT }
      );

      if ((await relatedcis).length < 1) {
        await CIRelationCisModel.create({
          parentci_id: parentCIUUID,
          parentci_classname: parentClassType,
          relationship_name: relationshipname,
          childci_id: childCIUUID,
          childci_classname: childClassType,
          clientid: clientId,
          created_by,
        });
      }
    }
  };
  //----------------------------------------------------------

  static insertAdditionalTables = async (additional_data: any, unique_id: any, ci_category: any, ipaddress: any, client_id: any) => {

    if (Boolean(additional_data.inbound_connections) || additional_data.inbound_connections == '') {

      await sequelize.query(`delete from con_cmdb.con_cmdb_inbound_connections where unique_id ='${unique_id}' and client_id ='${client_id}' and manually_created = false `, { type: Sequelize.QueryTypes.DELETE })
      if (additional_data.inbound_connections.length > 0) {
        for (let i = 0; i < additional_data.inbound_connections.length; i++) {
          logger.info(additional_data.inbound_connections[i], ">>>>>")
          let data: any = {}
          data.unique_id = unique_id
          data.ipaddress = ipaddress
          data.ci_category = ci_category
          data.client_id = client_id
          data.localport = additional_data.inbound_connections[i].LocalPort
          data.servicename = additional_data.inbound_connections[i].ServiceName
          data.processname = additional_data.inbound_connections[i].ProcessName
          data.remoteci_port = additional_data.inbound_connections[i].RemotePort
          data.remoteci_ipaddress = additional_data.inbound_connections[i].ForeignAddress
          data.connection_protocol = additional_data.inbound_connections[i].ConnectionType
          const keys1 = Object.keys(data).toString();
          const values1 = Object.values(data)

          const combinedValues1 = values1.map((value: any) => {
            const findingString =
              typeof value !== "boolean" &&
              typeof value !== "number" &&
              typeof value !== "object" &&
              value !== null;
            return findingString ? `'${value}'` : value;
          });
          for (let index = 0; index < combinedValues1.length; index++) {
            if (
              typeof combinedValues1[index] == "object" &&
              combinedValues1[index] !== null
            ) {
              combinedValues1[index] =
                "'" + JSON.stringify(combinedValues1[index]) + "'";
            } else if (combinedValues1[index] == null) {
              combinedValues1[index] = "null";
            }
          }

          await sequelize.query(
            `INSERT INTO con_cmdb.con_cmdb_inbound_connections (${keys1}) VALUES (${combinedValues1}) `,
            { type: Sequelize.QueryTypes.INSERT }
          );

        }
      }

    }
    if (Boolean(additional_data.outbound_connections) || additional_data.outbound_connections == '') {


      await sequelize.query(`delete from con_cmdb.con_cmdb_outbound_connections where unique_id ='${unique_id}' and client_id ='${client_id}' and manually_created = false`, { type: Sequelize.QueryTypes.DELETE })

      if (additional_data.outbound_connections.length > 0) {
        for (let i = 0; i < additional_data.outbound_connections.length; i++) {
          logger.info(additional_data.outbound_connections[i], ">>>>>")
          let data: any = {}
          data.unique_id = unique_id
          data.ipaddress = ipaddress
          data.ci_category = ci_category
          data.client_id = client_id
          data.localport = additional_data.outbound_connections[i].LocalPort
          data.servicename = additional_data.outbound_connections[i].ServiceName
          data.processname = additional_data.outbound_connections[i].ProcessName
          data.remoteci_port = additional_data.outbound_connections[i].RemotePort
          data.remoteci_ipaddress = additional_data.outbound_connections[i].ForeignAddress
          data.connection_protocol = additional_data.outbound_connections[i].ConnectionType
          const keys1 = Object.keys(data).toString();
          const values1 = Object.values(data)
          const combinedValues1 = values1.map((value: any) => {
            const findingString =
              typeof value !== "boolean" &&
              typeof value !== "number" &&
              typeof value !== "object" &&
              value !== null;
            return findingString ? `'${value}'` : value;
          });
          for (let index = 0; index < combinedValues1.length; index++) {
            if (
              typeof combinedValues1[index] == "object" &&
              combinedValues1[index] !== null
            ) {
              combinedValues1[index] =
                "'" + JSON.stringify(combinedValues1[index]) + "'";
            } else if (combinedValues1[index] == null) {
              combinedValues1[index] = "null";
            }
          }

          await sequelize.query(
            `INSERT INTO con_cmdb.con_cmdb_outbound_connections (${keys1}) VALUES (${combinedValues1}) `,
            { type: Sequelize.QueryTypes.INSERT }
          );

        }
      }

    }
    if (Boolean(additional_data.running_processes) || additional_data.running_processes == '') {


      await sequelize.query(`delete from con_cmdb.con_cmdb_running_processes where unique_id ='${unique_id}' and client_id ='${client_id}' and manually_created = false`, { type: Sequelize.QueryTypes.DELETE })

      if (additional_data.running_processes.length > 0) {
        for (let i = 0; i < additional_data.running_processes.length; i++) {
          logger.info(additional_data.running_processes[i], ">>>>>")
          let data: any = {}
          data.unique_id = unique_id
          data.ipaddress = ipaddress
          data.ci_category = ci_category
          data.client_id = client_id
          data.processid = additional_data.running_processes[i].ProcessId
          data.servicename = additional_data.running_processes[i].ServiceName
          data.processname = additional_data.running_processes[i].ProcessName
          data.command = additional_data.running_processes[i].Command
          data.description = additional_data.running_processes[i].Description

          const keys1 = Object.keys(data).toString();
          const values1 = Object.values(data)
          const combinedValues1 = values1.map((value: any) => {
            const findingString =
              typeof value !== "boolean" &&
              typeof value !== "number" &&
              typeof value !== "object" &&
              value !== null;
            return findingString ? `'${value}'` : value;
          });
          for (let index = 0; index < combinedValues1.length; index++) {
            if (
              typeof combinedValues1[index] == "object" &&
              combinedValues1[index] !== null
            ) {
              combinedValues1[index] =
                "'" + JSON.stringify(combinedValues1[index]) + "'";
            } else if (combinedValues1[index] == null) {
              combinedValues1[index] = "null";
            }
          }

          await sequelize.query(
            `INSERT INTO con_cmdb.con_cmdb_running_processes (${keys1}) VALUES (${combinedValues1}) `,
            { type: Sequelize.QueryTypes.INSERT }
          );

        }
      }

    }
    if (Boolean(additional_data.installed_packages) || additional_data.installed_packages == '') {


      await sequelize.query(`delete from con_cmdb.con_cmdb_installed_packages where unique_id ='${unique_id}' and client_id ='${client_id}' and manually_created =false`, { type: Sequelize.QueryTypes.DELETE })

      if (additional_data.installed_packages.length > 0) {
        for (let i = 0; i < additional_data.installed_packages.length; i++) {
          logger.info(additional_data.installed_packages[i], ">>>>>")
          let data: any = {}
          data.unique_id = unique_id
          data.ipaddress = ipaddress
          data.ci_category = ci_category
          data.client_id = client_id
          data.application_name = additional_data.installed_packages[i].ApplicationName
          data.version = additional_data.installed_packages[i].SoftwareVersion
          data.installed_date = additional_data.installed_packages[i].InstalledDate
          data.manufacturer = additional_data.installed_packages[i].ServerName

          const keys1 = Object.keys(data).toString();
          const values1 = Object.values(data)
          const combinedValues1 = values1.map((value: any) => {
            const findingString =
              typeof value !== "boolean" &&
              typeof value !== "number" &&
              typeof value !== "object" &&
              value !== null;
            return findingString ? `'${value}'` : value;
          });
          for (let index = 0; index < combinedValues1.length; index++) {
            if (
              typeof combinedValues1[index] == "object" &&
              combinedValues1[index] !== null
            ) {
              combinedValues1[index] =
                "'" + JSON.stringify(combinedValues1[index]) + "'";
            } else if (combinedValues1[index] == null) {
              combinedValues1[index] = "null";
            }
          }

          await sequelize.query(
            `INSERT INTO con_cmdb.con_cmdb_installed_packages (${keys1}) VALUES (${combinedValues1}) `,
            { type: Sequelize.QueryTypes.INSERT }
          );

        }
      }

    }
    if (Boolean(additional_data.listening_ports) || additional_data.listening_ports == '') {


      await sequelize.query(`delete from con_cmdb.con_cmdb_listening_ports where unique_id ='${unique_id}' and client_id ='${client_id}' and manually_created =false`, { type: Sequelize.QueryTypes.DELETE })

      if (additional_data.listening_ports.length > 0) {
        for (let i = 0; i < additional_data.listening_ports.length; i++) {
          logger.info(additional_data.listening_ports[i], ">>>>>")
          let data: any = {}
          data.unique_id = unique_id
          data.ipaddress = ipaddress
          data.ci_category = ci_category
          data.client_id = client_id
          data.localport = additional_data.listening_ports[i].LocalPort
          data.servicename = additional_data.listening_ports[i].ServiceName
          data.processname = additional_data.listening_ports[i].ProcessName
          data.connection_protocol = additional_data.listening_ports[i].ConnectionType

          const keys1 = Object.keys(data).toString();
          const values1 = Object.values(data)
          const combinedValues1 = values1.map((value: any) => {
            const findingString =
              typeof value !== "boolean" &&
              typeof value !== "number" &&
              typeof value !== "object" &&
              value !== null;
            return findingString ? `'${value}'` : value;
          });
          for (let index = 0; index < combinedValues1.length; index++) {
            if (
              typeof combinedValues1[index] == "object" &&
              combinedValues1[index] !== null
            ) {
              combinedValues1[index] =
                "'" + JSON.stringify(combinedValues1[index]) + "'";
            } else if (combinedValues1[index] == null) {
              combinedValues1[index] = "null";
            }
          }

          await sequelize.query(
            `INSERT INTO con_cmdb.con_cmdb_listening_ports (${keys1}) VALUES (${combinedValues1}) `,
            { type: Sequelize.QueryTypes.INSERT }
          );

        }
      }

    }

  }

  //---------------------------------------------------------------

  static getCIAdditionalTablesData = async (req: Request, res: Response) => {
    try {

      let { unique_id, client_id, additional_tables, trimmed_response, q, search_table } = req.body;
      const lang = req.headers.lang ? req.headers.lang : "en";
      const response: any = await getLocalString(lang);
      const additional_data: any = {}
      if (!Boolean(trimmed_response)) {
        trimmed_response = false
      }
      if (additional_tables.length == 0) {
        return res.send({
          additional_tables: null,
          message: response.info_NoAdditionalTablesDataProvided
        })
      }


      for (let index = 0; index < additional_tables.length; index++) {
        const element = additional_tables[index];
        if (element == 'inbound_connections') {
          let selectClause
          const allColumns = [
            { column_name: "localport", column_type: "integer" },
            { column_name: "remoteci_port", column_type: "integer" },
            { column_name: "processname", column_type: "string" },
            { column_name: "servicename", column_type: "string" }
          ];

          const searchClause = (q: string) => {
            return allColumns
              .map((data, index, array) => {
                if (index === array.length - 1) {
                  if ((typeof q == 'number') && data.column_type == 'integer') {
                    return `${data.column_name} = ${q}`
                  } else if ((typeof q == 'string') && data.column_type == "string") {
                    return `${data.column_name} ILIKE '%${q}%'`;
                  } else if ((typeof q == 'number') && data.column_type == "string") {
                    return `${data.column_name} ILIKE '%${q}%'`;
                  }
                } else {
                  if ((typeof q == 'number') && data.column_type == 'integer') {
                    return `${data.column_name} = ${q} OR`
                  } else if ((typeof q == 'string') && data.column_type == "string") {
                    return `${data.column_name} ILIKE '%${q}%' OR`;
                  }
                }
              })
              .join(" ");
          };

          let replacements: any = {}
          if (trimmed_response == true) {
            selectClause = `select ipaddress,servicename,remoteci_ipaddress,remoteci_port from con_cmdb.con_cmdb_inbound_connections where unique_id = :unique_id and client_id = :client_id`
            replacements = { client_id: client_id, unique_id: unique_id }
          } else {
            if (q && search_table == 'inbound_connections') {
              selectClause = `select * from con_cmdb.con_cmdb_inbound_connections where (${searchClause(':q')}) and unique_id = :unique_id and client_id = :client_id`
              replacements = { client_id: client_id, unique_id: unique_id, q: q }
            } else {
              selectClause = `select * from con_cmdb.con_cmdb_inbound_connections where unique_id = :unique_id and client_id = :client_id`
              replacements = { client_id: client_id, unique_id: unique_id }
            }
          }
          const inbound_connections = await sequelize.query(selectClause, { replacements: replacements, type: Sequelize.QueryTypes.SELECT })
          additional_data.inbound_connections = inbound_connections
        }
        if (element == 'outbound_connections') {
          let selectClause
          const allColumns = [
            { column_name: "localport", column_type: "integer" },
            { column_name: "remoteci_port", column_type: "integer" },
            { column_name: "processname", column_type: "string" },
            { column_name: "servicename", column_type: "string" }
          ];

          const searchClause = (q: string) => {
            return allColumns
              .map((data, index, array) => {
                if (index === array.length - 1) {
                  if ((typeof q == 'number') && data.column_type == 'integer') {
                    return `${data.column_name} = ${q}`
                  } else if ((typeof q == 'string') && data.column_type == "string") {
                    return `${data.column_name} ILIKE '%${q}%'`;
                  } else if ((typeof q == 'number') && data.column_type == "string") {
                    return `${data.column_name} ILIKE '%${q}%'`;
                  }
                } else {
                  if ((typeof q == 'number') && data.column_type == 'integer') {
                    return `${data.column_name} = ${q} OR`
                  } else if ((typeof q == 'string') && data.column_type == "string") {
                    return `${data.column_name} ILIKE '%${q}%' OR`;
                  }
                }
              })
              .join(" ");
          };

          let replacements: any = {}
          if (trimmed_response == true) {
            selectClause = `select ipaddress,servicename,remoteci_ipaddress,remoteci_port from con_cmdb.con_cmdb_outbound_connections where unique_id = :unique_id and client_id = :client_id`
            replacements = { client_id: client_id, unique_id: unique_id }
          } else {
            if (q && search_table == 'outbound_connections') {
              selectClause = `select * from con_cmdb.con_cmdb_outbound_connections where (${searchClause(':q')}) and unique_id = :unique_id and client_id = :client_id`
              replacements = { client_id: client_id, unique_id: unique_id, q: q }
            } else {
              selectClause = `select * from con_cmdb.con_cmdb_outbound_connections where unique_id = :unique_id and client_id = :client_id`
              replacements = { client_id: client_id, unique_id: unique_id }
            }
          }
          const outbound_connections = await sequelize.query(selectClause, { replacements: replacements, type: Sequelize.QueryTypes.SELECT })
          additional_data.outbound_connections = outbound_connections

        }
        if (element == 'running_processes') {
          let selectClause
          const allColumns = [
            { column_name: "processid", column_type: "integer" },
            { column_name: "processname", column_type: "string" },
            { column_name: "servicename", column_type: "string" }
          ];

          const searchClause = (q: string) => {
            return allColumns
              .map((data, index, array) => {
                if (index === array.length - 1) {
                  if ((typeof q == 'number') && data.column_type == 'integer') {
                    return `${data.column_name} = ${q}`
                  } else if ((typeof q == 'string') && data.column_type == "string") {
                    return `${data.column_name} ILIKE '%${q}%'`;
                  } else if ((typeof q == 'number') && data.column_type == "string") {
                    return `${data.column_name} ILIKE '%${q}%'`;
                  }
                } else {
                  if ((typeof q == 'number') && data.column_type == 'integer') {
                    return `${data.column_name} = ${q} OR`
                  } else if ((typeof q == 'string') && data.column_type == "string") {
                    return `${data.column_name} ILIKE '%${q}%' OR`;
                  }
                }
              })
              .join(" ");
          };

          let replacements: any = {}
          if (trimmed_response == true) {
            selectClause = `select ipaddress,processid,processname,servicename,command,description from con_cmdb.con_cmdb_running_processes where unique_id = :unique_id and client_id = :client_id`
            replacements = { client_id: client_id, unique_id: unique_id }
          } else {
            if (q && search_table == 'running_processes') {
              selectClause = `select * from con_cmdb.con_cmdb_running_processes where (${searchClause(':q')}) and unique_id = :unique_id and client_id = :client_id`
              replacements = { client_id: client_id, unique_id: unique_id, q: q }
            } else {
              selectClause = `select * from con_cmdb.con_cmdb_running_processes where unique_id = :unique_id and client_id = :client_id`
              replacements = { client_id: client_id, unique_id: unique_id }
            }
          }
          const running_processes = await sequelize.query(selectClause, { replacements: replacements, type: Sequelize.QueryTypes.SELECT })
          additional_data.running_processes = running_processes
        }
        if (element == 'installed_packages') {
          let selectClause
          const allColumns = [
            "application_name"
          ];

          const searchClause = (q: string) => {
            return allColumns
              .map((data, index, array) => {
                if (index === array.length - 1) {
                  return `${data} ILIKE '%${q}%'`;
                } else {
                  return `${data} ILIKE '%${q}%' OR`;
                }
              })
              .join(" ");
          };

          let replacements: any = {}
          if (trimmed_response == true) {
            selectClause = `select application_name,version,installed_date,manufacturer from con_cmdb.con_cmdb_installed_packages where unique_id = :unique_id and client_id = :client_id`
            replacements = { client_id: client_id, unique_id: unique_id }
          } else {
            if (q && search_table == 'installed_packages') {
              selectClause = `select * from con_cmdb.con_cmdb_installed_packages where (${searchClause(':q')}) and unique_id = :unique_id and client_id = :client_id`
              replacements = { client_id: client_id, unique_id: unique_id, q: q }
            } else {
              selectClause = `select * from con_cmdb.con_cmdb_installed_packages where unique_id = :unique_id and client_id = :client_id`
              replacements = { client_id: client_id, unique_id: unique_id }
            }
          }
          const installed_packages = await sequelize.query(selectClause, { replacements: replacements, type: Sequelize.QueryTypes.SELECT })
          additional_data.installed_packages = installed_packages
        }
        if (element == 'listening_ports') {
          let selectClause
          const allColumns = [
            { column_name: "localport", column_type: "integer" },
            { column_name: "processname", column_type: "string" },
            { column_name: "servicename", column_type: "string" }
          ];

          const searchClause = (q: string) => {
            return allColumns
              .map((data, index, array) => {
                if (index === array.length - 1) {
                  if ((typeof q == 'number') && data.column_type == 'integer') {
                    return `${data.column_name} = ${q}`
                  } else if ((typeof q == 'string') && data.column_type == "string") {
                    return `${data.column_name} ILIKE '%${q}%'`;
                  } else if ((typeof q == 'number') && data.column_type == "string") {
                    return `${data.column_name} ILIKE '%${q}%'`;
                  }
                } else {
                  if ((typeof q == 'number') && data.column_type == 'integer') {
                    return `${data.column_name} = ${q} OR`
                  } else if ((typeof q == 'string') && data.column_type == "string") {
                    return `${data.column_name} ILIKE '%${q}%' OR`;
                  }
                }
              })
              .join(" ");
          };

          let replacements: any = {}
          if (trimmed_response == true) {
            selectClause = `select localport,processname,servicename,connection_protocol from con_cmdb.con_cmdb_listening_ports where unique_id = :unique_id and client_id = :client_id`
            replacements = { client_id: client_id, unique_id: unique_id }
          } else {
            if (q && search_table == 'listening_ports') {
              selectClause = `select * from con_cmdb.con_cmdb_listening_ports where (${searchClause(':q')}) and unique_id = :unique_id and client_id = :client_id`
              replacements = { client_id: client_id, unique_id: unique_id, q: q }
            } else {
              selectClause = `select * from con_cmdb.con_cmdb_listening_ports where unique_id = :unique_id and client_id = :client_id`
              replacements = { client_id: client_id, unique_id: unique_id }
            }
          }
          const listening_ports = await sequelize.query(selectClause, { replacements: replacements, type: Sequelize.QueryTypes.SELECT })
          additional_data.listening_ports = listening_ports
        }

      }
      return res.status(200).send({ additional_tables: additional_data })
    } catch (error) {
      logger.error(error)
      res.json({ error: error })

    }
  }


  static createCIs = async (
    req: Request,
    res: Response,
    next: NextFunction
  ) => {
    try {
      const clientId = req.body.clientid;
      const toplevelClasstype = req.body.citype;
      const created_by = req.body.attributes.created_by;
      const ip_address = req.body.attributes.private_ip
      const lang = req.headers.lang ? req.headers.lang : "en";
      const response: any = await getLocalString(lang);
      //get the identification rules matching CItype, clientid, datasource
      // identificationwhereclouse = CItype

      let _items = {};
      let result: any = [];
      let auditRequest: any = {}


      try {
        logger.info("creating new CIs into db...");
        const items =
          await CIIdentificationController._getCIIdentificationByIdForCIs(
            clientId,
            toplevelClasstype
          );
        if (items === false) {
          return next("Not Found");
        }

        _items = items;
        logger.info(`${_items}>>>>>>>>>>>>`);
      } catch (error: any) {
        logger.error(new Error(error));
        return next(error);
      }

      const identificationDef: any = _items;
      let toplevelCIUUID = null;
      let ciExists;

      if (identificationDef.length > 0) {
        // toplevelCIUUID = await this.createOrUpdateCI(toplevelClasstype, req.body.attributes, clientId, identificationDef[0]);
        const topCIResult = await this.createOrUpdateCI(
          toplevelClasstype,
          req.body.attributes,
          clientId,
          identificationDef[0]
        );

        toplevelCIUUID = topCIResult.ciUUID;
        ciExists = topCIResult.ciExists;
        if (auditLogsEnabled === 'true') {
          auditRequest = {
            client_id: req.headers.clientid,
            service_name: "cmdb-ciclass-manager",
            module: "Configuration-Item",
            db_table: "con_cmdb_configurationitem",
            description: ciExists ? "Configuration Item Updated successfully" : "Configuration Item created successfully",
            post_audit_data: req.body,
            action: ciExists ? "Update" : "Create",
            performed_by: req.headers.username,
            action_date: actionDate.format('YYYY-MM-DD'),
            user_id: req.headers.userid,
            realm: req.headers?.realm,
            apitoken: req.headers?.apitoken
          }
        }

        if (toplevelCIUUID === null || toplevelCIUUID === "") {
          return res.status(200).send({
            message: response.info_ciNotCreatedOrUpdated,
            success: false,
          });
        }

        logger.info(
          `toplevelCIUUID =>> ${topCIResult}, ${toplevelClasstype}, ${req.body.attributes}, ${clientId}, ${identificationDef[0]}, ${toplevelCIUUID}`
        );
        const childCIs = req.body.childcis;
        // const parentCIUUID = toplevelCIUUID;
        // const parentcitype = toplevelClasstype;
        let ci_uuid_mapping: any = new Map();
        ci_uuid_mapping.set(0, {
          uuid: toplevelCIUUID,
          citype: toplevelClasstype,
        });
        //if input payload contains key called additional_tables
        //then call insertAdditionalTables method(new method)-->(ci_uuid_mapping,additional_tables)
        const entry = ci_uuid_mapping.get(0);
        const additional_data = req.body.additional_tables
        const ipaddress = ip_address ? ip_address : null

        if (additional_data) {
          await this.insertAdditionalTables(additional_data, entry.uuid, entry.citype, ipaddress, clientId)
        }
        if (childCIs) {
          logger.info(`CHILD_CI : ${childCIs}`);
          for (let index = 0; index < childCIs.length; index++) {
            logger.info(`CHILD_CI inside loop: ${childCIs[index]}`);
            const childCI = childCIs[index];
            if (
              childCI.relationship !== undefined &&
              childCI.relationship.length > 0
            ) {
              const childCItype = childCI.citype;
              logger.info(`childCItype: ${childCItype}`);
              logger.info(`CLI: ${clientId}`);

              const direction = childCI.relationship_direction;

              const items =
                await CIIdentificationController._getCIIdentificationByIdForCIs(
                  clientId,
                  childCItype
                );
              if (items === false) {
                return next("Not Found");
              }
              const childIdentificationDef = items;
              logger.info(
                `childIdentificationDef==================> ${items} =========>`
              );
              if (childIdentificationDef.length > 0) {
                // const childCIUUID = await this.createOrUpdateCI(childCItype, childCI.attributes, clientId, childIdentificationDef[0]);
                const childResult = await this.createOrUpdateCI(
                  childCItype,
                  childCI.attributes,
                  clientId,
                  childIdentificationDef[0]
                );
                const childCIUUID = childResult.ciUUID;
                if (childCIUUID === null || childCIUUID === "") {
                  result.push({
                    message: response.info_childCINotCreatedOrUpdated,
                    success: false,
                  });
                } else {
                  ci_uuid_mapping.set(childCI.current_mapping_level, {
                    uuid: childCIUUID,
                    citype: childCItype,
                  });

                  result.push({
                    message: `${ciExists
                      ? response.info_childCIUpdatedSuccessfully
                      : response.info_childCICreatedSuccessfully
                      }`,
                    success: true,
                    unique_id: childCIUUID,
                    ci_name: childCI.attributes.display_name,
                    ci_type: childCI.citype,
                  });
                  let parentcitype = "";
                  let parentCIUUID = "";
                  if (
                    !Boolean(childCI.parent_mapping_level) ||
                    childCI.parent_mapping_level == null
                  ) {
                    parentCIUUID = ci_uuid_mapping.get(0).uuid;
                    parentcitype = ci_uuid_mapping.get(0).citype;
                  } else {
                    parentCIUUID = ci_uuid_mapping.get(
                      childCI.parent_mapping_level
                    ).uuid;
                    parentcitype = ci_uuid_mapping.get(
                      childCI.parent_mapping_level
                    ).citype;
                    logger.info(`parentcitype: ${parentcitype}`);
                  }

                  if (direction === "parent-to-child") {
                    this.createOrUpdateRelationship(
                      parentCIUUID,
                      parentcitype,
                      childCI.relationship,
                      childCIUUID,
                      childCItype,
                      clientId,
                      created_by
                    );
                  }
                  if (direction === "child-to-parent") {
                    this.createOrUpdateRelationship(
                      childCIUUID,
                      childCItype,
                      childCI.relationship,
                      parentCIUUID,
                      parentcitype,
                      clientId,
                      created_by
                    );
                  }
                }
              }
            }
          }
        }
      }
      logger.info(`${toplevelCIUUID}, ${identificationDef.length} >>>>>>>`);
      if (ciExists) {
        if (auditLogsEnabled === 'true') {
          auditRequest.pre_audit_data = { "toplevelCIUUID": toplevelCIUUID, "ciExists": ciExists };
          await auditLogsController.createauditDetails(auditRequest);
        }
      } else {
        if (auditLogsEnabled === 'true') {
          await auditLogsController.createauditDetails(auditRequest);
        }
      }
      return res.status(200).send({
        message: `${ciExists ? response.info_ciUpdatedSuccessfully : response.info_ciCreatedSuccessfully
          }`,
        success: true,
        unique_id: toplevelCIUUID,
        ci_name: req.body.attributes.display_name,
        ci_type: req.body.citype,
        child_cis: result,
      });
    } catch (error: any) {
      logger.error(new Error(error));
      next(error);
    }
  };


  static createCIRelatedCI = async (
    req: Request,
    res: Response,
    next: NextFunction
  ) => {
    let auditRequest: any = {}
    if (auditLogsEnabled === 'true') {
      auditRequest = {
        client_id: req.headers.clientid,
        service_name: "cmdb-ciclass-manager",
        module: "related-CI",
        db_table: "con_cmdb_relatedcis",
        description: "CIRelatedCI Created Sucessfully",
        post_audit_data: req.body,
        action: "Create",
        performed_by: req.headers.username,
        action_date: actionDate.format('YYYY-MM-DD'),
        user_id: req.headers.userid,
        realm: req.headers?.realm,
        apitoken: req.headers?.apitoken
      }
    }
    try {
      const {
        parentci_id,
        parentci_classname,
        relationship_name,
        childci_id,
        childci_classname,
        clientid,
        relationship_direction,
      } = req.body;
      const lang = req.headers.lang ? req.headers.lang : "en";
      const response: any = await getLocalString(lang);
      logger.info(`creating CI Related CI for client id: ${clientid}`);
      if (relationship_direction === "parent-to-child") {
        this.createOrUpdateRelationship(
          parentci_id,
          parentci_classname,
          relationship_name,
          childci_id,
          childci_classname,
          clientid,
          ""
        );
      }
      if (relationship_direction === "child-to-parent") {
        this.createOrUpdateRelationship(
          childci_id,
          childci_classname,
          relationship_name,
          parentci_id,
          parentci_classname,
          clientid,
          ""
        );
      }

      if (auditLogsEnabled === 'true') {
        await auditLogsController.createauditDetails(auditRequest);
      }
      return res.status(200).send({
        message: response.info_createdSuccessfully,
        success: true,
      });
    } catch (error: any) {
      if (auditLogsEnabled === 'true') {
        auditRequest.description = ' CIRelatedCI Create Operation Failed'
        auditRequest.error_description = `500 - Internal Server Error due to ${error.message} `;
        await auditLogsController.createauditDetails(auditRequest);
      }
      logger.error(new Error(error));
      next(error);
    }
  };

  static deleteRelationship = async (
    parentCIUUID: any,
    parentClassType: any,
    relationshipname: any,
    childCIUUID: string,
    childClassType: any,
    clientId: string
  ) => {
    //check the relatedCIs table with where clause parentciid=parentCIUUID and relationshipname=relationshipname and childciid=childCIUUID

    const whereClause = `parentci_id='${parentCIUUID}' and relationship_name='${relationshipname}' and childci_id='${childCIUUID}'`;

    const relatedcis = await sequelize.query(
      `
            delete from con_cmdb.con_cmdb_relatedcis where ${whereClause}
        `,
      { type: Sequelize.QueryTypes.SELECT }
    );
  };

  static deleteCIRelatedCI = async (
    req: Request,
    res: Response,
    next: NextFunction
  ) => {
    let auditRequest: any = {}
    if (auditLogsEnabled === 'true') {
      auditRequest = {
        client_id: req.headers.clientid,
        service_name: "cmdb-ciclass-manager",
        module: "related-CI",
        db_table: "con_cmdb_relatedcis",
        description: "CIRelatedCI Deleted Sucessfully",
        post_audit_data: req.body,
        action: "Delete",
        performed_by: req.headers.username,
        action_date: actionDate.format('YYYY-MM-DD'),
        user_id: req.headers.userid,
        realm: req.headers?.realm,
        apitoken: req.headers?.apitoken
      }
    }
    try {
      const {
        parentci_id,
        parentci_classname,
        relationship_name,
        childci_id,
        childci_classname,
        clientid,
        relationship_direction,
      } = req.body;
      const lang = req.headers.lang ? req.headers.lang : "en";
      const response: any = await getLocalString(lang);
      logger.info(`deleting CI Related CI for client id: ${clientid}`);
      if (relationship_direction === "parent-to-child") {
        this.deleteRelationship(
          parentci_id,
          parentci_classname,
          relationship_name,
          childci_id,
          childci_classname,
          clientid
        );
      }
      if (relationship_direction === "child-to-parent") {
        this.deleteRelationship(
          childci_id,
          childci_classname,
          relationship_name,
          parentci_id,
          parentci_classname,
          clientid
        );
      }


      if (auditLogsEnabled === 'true') {
        await auditLogsController.createauditDetails(auditRequest);
      }
      return res.status(200).send({
        message: response.info_deletedSuccessfully,
        success: true,
      });
    } catch (error: any) {
      logger.error(new Error(error));
      next(error);
    }
  };

  static queryProcessor = (queryAttributes: any, qualifier: string) => {
    logger.info(`${queryAttributes} ...........`);
    return queryAttributes
      .map((qAttr: any) => {
        if (
          (qAttr.subExpStart !== undefined || qAttr.subExpStart !== null) &&
          (qAttr.nextOperator !== undefined || qAttr.nextOperator !== null) &&
          (qAttr.attr !== undefined || qAttr.attr !== null) &&
          (qAttr.operator !== undefined || qAttr.operator !== null) &&
          (qAttr.value !== undefined || qAttr.value !== null) &&
          (qAttr.nextOperator !== undefined || qAttr.nextOperator !== null)
        )
          if (Boolean(qAttr.attr)) {
            qAttr.attr = `${qualifier}.con_cmdb_${qAttr.attr}`;
          }
        return Object.values(qAttr).join(" ");
      })
      .join(" ");
  };
  static getCIsbyTypeByCloudAccount = async (
    req: Request,
    res: Response,
    next: NextFunction
  ) => {
    let { cloud_type, ci_type, cloud_account, client_id } = req.body as any;
    const lang = req.headers.lang ? req.headers.lang : "en";
    const response: any = await getLocalString(lang);
    logger.info(`fetching CIs by Type By Cloud Account for client id: ${client_id}`);
    // if (!db_table_names.includes(ci_type.toLowerCase())) {
    //   return next(new Error('Dbname not exists in the database'))
    // }
    if (cloud_type == "aws") {
      let data = <any>(
        await sequelize.query(
          `SELECT con_cmdb_region,con_cmdb_unique_id from con_cmdb.con_cmdb_${ci_type.toLowerCase()} WHERE con_cmdb_clientid= :client_id AND con_cmdb_account_id= :cloud_account`,
          { replacements: { client_id: client_id, cloud_account: cloud_account }, type: Sequelize.QueryTypes.SELECT }
        )
      );
      if (data.length) {
        return res.status(200).send({
          message: response.info_CIDataFetchedSuccessfully,
          success: true,
          items: data,
        });
      } else {
        return res.status(200).send({
          message: response.info_dataNotFound,
          success: false,
        });
      }
    }

    if (cloud_type == "azure") {
      let data = <any>(
        await sequelize.query(
          `SELECT con_cmdb_region,con_cmdb_unique_id from con_cmdb.con_cmdb_${ci_type.toLowerCase()} WHERE con_cmdb_clientid= :client_id AND con_cmdb_subscription= :cloud_account`,
          { replacements: { client_id: client_id, cloud_account: cloud_account }, type: Sequelize.QueryTypes.SELECT }
        )
      );

      if (data.length) {
        return res.status(200).send({
          message: response.info_CIDataFetchedSuccessfully,
          success: true,
          items: data,
        });
      } else {
        return res.status(200).send({
          message: response.info_dataNotFound,
          success: false,
        });
      }
    }
    if (cloud_type == "gcp") {
      let data = <any>(
        await sequelize.query(
          `SELECT con_cmdb_zone AS con_cmdb_region,con_cmdb_unique_id from con_cmdb.con_cmdb_${ci_type.toLowerCase()} WHERE con_cmdb_clientid= :client_id AND con_cmdb_gcpproject= :cloud_account`,
          { replacements: { client_id: client_id, cloud_account: cloud_account }, type: Sequelize.QueryTypes.SELECT }
        )
      );
      if (data.length) {
        return res.status(200).send({
          message: response.info_CIDataFetchedSuccessfully,
          success: true,
          items: data,
        });
      } else {
        return res.status(200).send({
          message: response.info_dataNotFound,
          success: false,
        });
      }
    }
  };

  static getManagedCIsByQuery = async (
    req: Request,
    res: Response,
    next: NextFunction
  ) => {
    try {
      let parentWhereClause = "";
      let childWhereClause = "";
      let relatedWhereClause = "";
      let joinClause1 = "";
      let joinClause2 = "";
      let attributelist: any = [];
      let finalSQLQuery = "";
      const MIN_SIZE = 10;
      const defaultpagination = true;
      let countclause = "";

      const allColumns = ["p.con_cmdb_display_name", "p.con_cmdb_private_ip"];
      const searchClause = (q: string) => {
        return allColumns
          .map((data, index, array) => {
            if (index === array.length - 1) {
              return `${data} ILIKE '%${q}%'`;
            } else {
              return `${data} ILIKE '%${q}%' OR`;
            }
          })
          .join(" ");
      };

      const columnNamesAs = (columnName: string[]) => {
        return columnName.map((cName: string) => {
          return `${cName} AS ${cName.replace(/p.con_cmdb_/, "")}`;
        });
      };

      const childcolumnNamesAs = (columnName: string[]) => {
        return columnName.map((cName: string) => {
          return `${cName} AS ${cName.replace(/c.con_cmdb_/, "child_")}`;
        });
      };

      const {
        clientid,
        citype,
        size = MIN_SIZE,
        depth = 1,
        page = 1,
        pagination = defaultpagination,
        q,
      } = req.body;
      const query_attrs = req.body.query_attrs;
      const resultattrs = req.body.result_attrs;
      const parentattrList = resultattrs
        .split(",")
        .map((attr: string) => `p.con_cmdb_${attr}`);
      const resultattrList = columnNamesAs(parentattrList);
      logger.info(`fetching Managed CIs By Query for client id: ${clientid}`);
      parentWhereClause = `p.con_cmdb_ismanagedci=true and p.con_cmdb_clientid='${clientid}'`;
      //if query attrs is undefined or null or notempty then processess below
      if (query_attrs != undefined && query_attrs !== null) {
        const query_expression = this.queryProcessor(query_attrs, "p");
        logger.info(`query expression==> ${query_expression}`);
        parentWhereClause = `${parentWhereClause} and (${query_expression})`;
        logger.info(`parentWhereClause: ${parentWhereClause}`);
      }
      const childcis = req.body.childcis;
      let childCIExist = false;
      // if (childcis is not undefined or not null) {  //condition to be corrected
      if (childcis !== undefined && childcis !== null) {
        childCIExist = true;
        const childci = childcis[0];
        const childcitype = childci.citype;
        const childresultattrs = childci.result_attrs;
        const childresultAttrList = childcolumnNamesAs(
          childresultattrs
            .split(",")
            .map((attr: string) => `c.con_cmdb_${attr}`)
        );
        logger.info(`childresultAttrList: ${childresultAttrList}`);

        // const combinedattrs = [...parentattrList, ...childresultAttrList]
        attributelist = [...resultattrList, ...childresultAttrList];

        if (childci.query_attrs != undefined && childci.query_attrs !== null) {
          const child_query_expression = this.queryProcessor(
            childci.query_attrs,
            "c"
          );
          logger.info(`child query expression==> ${child_query_expression}`);
          // childWhereClause = `(${child_query_expression})`
          childWhereClause = `${child_query_expression}`;
        }

        const relationship_name = childci.relationship_name;
        const relationship_direction = childci.relationship_direction;
        relatedWhereClause = `r.clientid='${clientid}' and r.relationship_name='${relationship_name}'`;

        if (relationship_direction == "parent-child") {
          joinClause1 = `inner join con_cmdb.con_cmdb_relatedcis as r on p.con_cmdb_unique_id = r.parentci_id`;
          joinClause2 = `inner join con_cmdb.con_cmdb_${childcitype} as c on r.childci_id = c.con_cmdb_unique_id`;
        } else if (relationship_direction == "child-parent") {
          joinClause1 = `inner join con_cmdb.con_cmdb_relatedcis as r on p.con_cmdb_unique_id = r.childci_id`;
          joinClause2 = `inner join con_cmdb.con_cmdb_${childcitype} as c on r.parentci_id = c.con_cmdb_unique_id`;
        }
      }
      if (childCIExist) {
        logger.info(`${attributelist} : attrList<===`);
        if (pagination == true) {
          finalSQLQuery = `select ${attributelist} from con_cmdb.con_cmdb_${citype} as p ${joinClause1} ${joinClause2} where ${parentWhereClause} and ${relatedWhereClause} and ${childWhereClause}
                LIMIT :size 
                OFFSET (:page - 1) * :size`;
        } else {
          finalSQLQuery = `select ${attributelist} from con_cmdb.con_cmdb_${citype} as p ${joinClause1} ${joinClause2} where ${parentWhereClause} and ${relatedWhereClause} and ${childWhereClause}`;
        }
        countclause = `select count(*) from con_cmdb.con_cmdb_${citype} as p ${joinClause1} ${joinClause2} where ${parentWhereClause} and ${relatedWhereClause} and ${childWhereClause}`;
      } else {
        let whereClause = q
          ? `${parentWhereClause} and (${searchClause(q)})`
          : parentWhereClause;
        if (pagination == true) {
          finalSQLQuery = `select ${resultattrList} from con_cmdb.con_cmdb_${citype} as p where ${whereClause}
                LIMIT :size 
                OFFSET (:page - 1) * :size`;
        } else {
          finalSQLQuery = `select ${resultattrList} from con_cmdb.con_cmdb_${citype} as p where ${whereClause}`;
        }
        countclause = `select count(*) from con_cmdb.con_cmdb_${citype} as p where ${whereClause}`;
      }
      logger.info(`${finalSQLQuery} >>>>`);
      const ciList: any = await sequelize.query(finalSQLQuery, {
        replacements: { page: page, size: size },
        type: Sequelize.QueryTypes.SELECT,
      });

      const [{ count }] = <any>await sequelize.query(countclause, {
        type: Sequelize.QueryTypes.SELECT,
      });

      const pages = Math.ceil(count / (size ? Number(size) : MIN_SIZE));

      return res.status(200).send({
        items: ciList,
        count,
        pages,
        page: Number(page),
        success: true,
      });
    } catch (error: any) {
      logger.error(new Error(error));
      next(error);
    }
  };

  // static deleteCIs = async (
  //   req: Request,
  //   res: Response,
  //   next: NextFunction
  // ) => {
  //   res.send({
  //     data: "sss",
  //   });
  // };

  static deleteUnmanagedCIById = async (
    req: Request,
    res: Response,
    next: NextFunction
  ) => {
    let auditRequest: any = {}
    if (auditLogsEnabled === 'true') {
      auditRequest = {
        client_id: req.headers.clientid,
        service_name: "cmdb-ciclass-manager",
        module: "Configuration-Item",
        db_table: "con_cmdb_configurationitem",
        description: "Configuration Item deleted successfully",
        post_audit_data: req.body,
        action: "Delete",
        performed_by: req.headers.username,
        action_date: actionDate.format('YYYY-MM-DD'),
        user_id: req.headers.userid,
        realm: req.headers?.realm,
        apitoken: req.headers?.apitoken
      }
    }
    try {
      const { unique_id, clientid } = req.body;
      const lang = req.headers.lang ? req.headers.lang : "en";
      const response: any = await getLocalString(lang);
      logger.info(`deleting Unmanaged CI By Id for client id: ${clientid}`);
      if (!Boolean(clientid)) return next(new Error(response.info_clientidRequired));
      if (!Boolean(unique_id)) return next(new Error(response.info_uniqueIDRequired));
      const curr_timestamp = moment().format("yyyy-MM-DD HH:mm:ss");
      logger.info(`${unique_id} >>>>>>>>`);
      for (let index = 0; index < unique_id.length; index++) {
        const uniqueIndex = unique_id[index]
        const getci = <any>await sequelize.query(
          `select * from con_cmdb.con_cmdb_configurationitem where con_cmdb_unique_id = :uniqueIndex
                and con_cmdb_clientid = :clientid`,
          {
            replacements: { clientid: clientid, uniqueIndex: uniqueIndex },
            type: Sequelize.QueryTypes.SELECT,
          }
        );
        if (getci.length < 1) return next(new Error(response.info_CIDoesNotExists));

        const ismanagedci = getci[0].con_cmdb_ismanagedci;
        const ci_category = getci[0].con_cmdb_ci_category.toLowerCase();
        // if (!db_table_names.includes(ci_category)) {
        //   return next(new Error('Dbname not exists in the database'))
        // }
        const fetchci = <any>await sequelize.query(
          `select * from con_cmdb.con_cmdb_${ci_category} where con_cmdb_unique_id = :uniqueIndex and con_cmdb_clientid = :clientid `,
          {
            replacements: { clientid: clientid, uniqueIndex: uniqueIndex },
            type: Sequelize.QueryTypes.SELECT,
          }
        );

        const ci_baselines = await sequelize.query(
          `select baseline_name, max_level from public.ci_baseline where clientid = :clientid and citype = :ci_category and is_enabled = true`,
          { replacements: { clientid: clientid, ci_category: ci_category }, type: Sequelize.QueryTypes.SELECT }
        );
        if (ismanagedci == true)
          return next(new Error(response.info_managedCIsCannontBeDeleted));
        if (ci_baselines.length == 0) {
          ci_baselines.push({ baseline_name: "default", max_level: 10 });
        }
        ci_baselines.forEach(async (baseline: any) => {
          fetchci[0].con_cmdb_baseline_name = baseline.baseline_name;
          fetchci[0].con_cmdb_ci_operation = "Delete";
          fetchci[0].con_cmdb_ct_created = curr_timestamp;
          fetchci[0].con_cmdb_last_modified_time = curr_timestamp;
          fetchci[0].con_cmdb_created_date = curr_timestamp;

          const keys = Object.keys(fetchci[0]).toString();
          const values = Object.values(fetchci[0]);
          // const combinedValues = values.map((value: any) => {
          //   const findingString =
          //     typeof value !== "boolean" &&
          //     typeof value !== "number" &&
          //     typeof value !== "object" &&
          //     value !== null;
          //   return findingString ? `'${value}'` : value;
          // });
          // for (let index = 0; index < combinedValues.length; index++) {
          //   if (
          //     typeof combinedValues[index] == "object" &&
          //     combinedValues[index] !== null
          //   ) {
          //     combinedValues[index] =
          //       "'" + JSON.stringify(combinedValues[index]) + "'";
          //   } else if (combinedValues[index] == null) {
          //     combinedValues[index] = "null";
          //   }
          // }
          const insertPlaceholders = values.map(() => "?").join(", ");
          await sequelize.query(
            `INSERT INTO con_cmdb.ct_con_cmdb_${ci_category} (${keys}) VALUES (${insertPlaceholders}) `,
            { replacements: values, type: Sequelize.QueryTypes.INSERT }
          );
        });

        const relatedwhereClause1 = `childci_id= :uniqueIndex and clientid= :clientid`;
        const relatedcis1: any = await sequelize.query(
          `
                       SELECT * from con_cmdb.con_cmdb_relatedcis where ${relatedwhereClause1}
                   `,
          { replacements: { clientid: clientid, uniqueIndex: uniqueIndex }, type: Sequelize.QueryTypes.SELECT }
        );
        const relatedwhereClause = `parentci_id= :uniqueIndex and clientid= :clientid`;
        const relatedcis: any = await sequelize.query(
          `SELECT * from con_cmdb.con_cmdb_relatedcis where ${relatedwhereClause}`,
          { replacements: { clientid: clientid, uniqueIndex: uniqueIndex }, type: Sequelize.QueryTypes.SELECT }
        );

        if (relatedcis.length > 0) {
          await sequelize.query(
            `delete from con_cmdb.con_cmdb_relatedcis where ${relatedwhereClause}`,
            { replacements: { clientid: clientid, uniqueIndex: uniqueIndex }, type: Sequelize.QueryTypes.DELETE }
          );
        }

        if (relatedcis1.length > 0) {
          let relationship_name;
          //for loop through i = 0 to length {
          for (let i = 0; i < relatedcis1.length; i++) {
            relationship_name = relatedcis1[i].relationship_name;
            const relationshipRecord: any = await sequelize.query(
              `select * from public.cmdb_relationships where relationship_name= :relationship_name and iscontained=true`,
              { replacements: { relationship_name: relationship_name }, type: Sequelize.QueryTypes.SELECT }
            );
            if (relationshipRecord.length > 0) {
              this.deleteCIById(
                relatedcis1[i].parentci_classname,
                relatedcis1[i].parentci_id,
                clientid
              );
            }
            const relatedci_id = relatedcis1[i].relatedci_id
            await sequelize.query(
              `delete from con_cmdb.con_cmdb_relatedcis where relatedci_id= :relatedci_id`,
              { replacements: { relatedci_id: relatedci_id }, type: Sequelize.QueryTypes.DELETE }
            );
          }
        }
        await sequelize.query(`delete from con_cmdb.con_cmdb_inbound_connections where unique_id = :uniqueIndex and client_id = :clientid `, { replacements: { clientid: clientid, uniqueIndex: uniqueIndex }, type: Sequelize.QueryTypes.DELETE })
        await sequelize.query(`delete from con_cmdb.con_cmdb_outbound_connections where unique_id = :uniqueIndex and client_id = :clientid`, { replacements: { clientid: clientid, uniqueIndex: uniqueIndex }, type: Sequelize.QueryTypes.DELETE })
        await sequelize.query(`delete from con_cmdb.con_cmdb_running_processes where unique_id = :uniqueIndex and client_id = :clientid`, { replacements: { clientid: clientid, uniqueIndex: uniqueIndex }, type: Sequelize.QueryTypes.DELETE })
        await sequelize.query(`delete from con_cmdb.con_cmdb_installed_packages where unique_id = :uniqueIndex and client_id = :clientid`, { replacements: { clientid: clientid, uniqueIndex: uniqueIndex }, type: Sequelize.QueryTypes.DELETE })
        await sequelize.query(`delete from con_cmdb.con_cmdb_listening_ports where unique_id = :uniqueIndex and client_id = :clientid`, { replacements: { clientid: clientid, uniqueIndex: uniqueIndex }, type: Sequelize.QueryTypes.DELETE })

        this.deleteCIById(ci_category, unique_id[index], clientid);
      }
      if (auditLogsEnabled === 'true') {
        await auditLogsController.createauditDetails(auditRequest);
      }
      return res.status(200).send({
        message: response.info_deletedSuccessfully,
        success: true,
      });
    } catch (error: any) {
      if (auditLogsEnabled === 'true') {
        auditRequest.description = 'Configuration Item Delete Operation Failed'
        auditRequest.error_description = `500 - Internal Server Error due to ${error.message} `;
        await auditLogsController.createauditDetails(auditRequest);
      }
      logger.error(new Error(error));
      next(error);
    }
  };

  static deleteCIById = async (
    ci_category: any,
    unique_id: any,
    clientid: any
  ) => {
    await sequelize.query(
      `
							delete from con_cmdb.con_cmdb_${ci_category} where con_cmdb_unique_id = '${unique_id}' and con_cmdb_clientid ='${clientid}'
							`,
      { type: Sequelize.QueryTypes.DELETE }
    );
  };

  static getRelatedRecords = async (req: Request, res: Response, next: NextFunction) => {
    try {
      let { realm }: any = req.headers;
      const headers = {
        realm: realm,
      };
      const reqData = Object.assign({}, req.body);
      const lang = req.headers.lang ? req.headers.lang : "en";
      const response: any = await getLocalString(lang);
      logger.info(`fetching Related Records...`);
      if (reqData.type) {
        if (reqData.type == "Incident") {
          delete reqData.type;
          reqData.featureId = "FE_61a75a142626ed0017844d3a";
          axios
            .post(`${TICKETSERVICEINCIDENTURL}`, reqData, {
              headers: headers,
            })
            .then((result) => {
              return res.status(200).send({
                success: true,
                data: result.data,
              });
            })
            .catch((error) => {
              logger.error(new Error(error));
            });
        }

        if (reqData.type == "SR") {
          delete reqData.type;
          reqData.featureId = "FE_61a75a202626ed0017844d3c";
          axios
            .post(`${TICKETSERVICESRURL}`, reqData, {
              headers: headers,
            })
            .then((result) => {
              return res.status(200).send({
                success: true,
                data: result.data,
              });
            })
            .catch((error) => {
              logger.error(new Error(error));
            });
        }
        if (reqData.type == "Change") {
          delete reqData.type;
          reqData.featureId = "FE_61a75a4b2626ed0017844d44";
          axios
            .post(`${TICKETSERVICESCHANGEURL}`, reqData, {
              headers: headers,
            })
            .then((result) => {
              logger.info(`+++++++change result++++++++: ${result}`);
              return res.status(200).send({
                success: true,
                data: result.data,
              });
            })
            .catch((error) => {
              logger.error(`change error : ${error}`);
            });
        }
      } else {
        return res.status(200).send({
          success: false,
          message: response.info_typeFieldShouldBeMandatory,
        });
      }
    } catch (error: any) {
      logger.error(new Error(error));
      next(error);
      // throw error;
    }
  };
  static getrunidentifiers = async (req: Request, res: Response) => {
    const { clientid } = req.body;
    let ciCategoryValues = req.body.ci_category.length
      ? req.body.ci_category.join("', '")
      : "";
    logger.info(`fetching run identifiers for client id: ${clientid}`);
    const runidentifier = await sequelize.query(
      `select distinct con_cmdb_discovery_runidentifier from con_cmdb.con_cmdb_configurationitem
      where  con_cmdb_clientid = :clientid and LOWER(con_cmdb_ci_category) IN(:ciCategoryValues) and con_cmdb_discovery_runidentifier is not null`,
      { replacements: { clientid: clientid, ciCategoryValues: ciCategoryValues.map((value: string) => { return value.toLowerCase() }) }, type: Sequelize.QueryTypes.SELECT }
    );

    return res.status(200).send({
      success: true,
      items: runidentifier,
    });
  };

  static getCIStatusCount = async (req: Request, res: Response) => {
    const { clientid, citype } = req.body;
    logger.info(`fetching CI Status Count for client id: ${clientid}`);
    const result = await sequelize.query(
      `
        select COUNT(con_cmdb_cistatus),con_cmdb_cistatus as cistatus from con_cmdb.con_cmdb_${citype.toLowerCase()}
        where  con_cmdb_clientid = :clientid GROUP BY con_cmdb_cistatus
        `,
      { replacements: { clientid: clientid }, type: Sequelize.QueryTypes.SELECT }
    );
    return res.status(200).send({
      success: true,
      items: result,
    });
  };

  static updateCIMigrationStatus = async (
    req: Request,
    res: Response,
    next: NextFunction
  ) => {
    const { clientid, unique_id, migration_status } = req.body;
    const lang = req.headers.lang ? req.headers.lang : "en";
    const response: any = await getLocalString(lang);
    logger.info(`updating CI Migration Status for client id: ${clientid}`);
    if (!Boolean(clientid)) return next(new Error(response.info_clientidRequired));
    if (!Boolean(unique_id)) return next(new Error(response.info_uniqueIDRequired));

    const fetchRecord = await sequelize.query(
      `select * from con_cmdb.con_cmdb_configurationitem where con_cmdb_clientid= :clientid AND con_cmdb_unique_id= :unique_id`,
      { replacements: { clientid: clientid, unique_id: unique_id }, type: Sequelize.QueryTypes.SELECT }
    );
    if (!Boolean(fetchRecord.length)) {
      {
        return res.status(200).send({
          message: response.info_dataNotFound,
          success: true,
        });
      }
    } else {
      const updatingStatus = await sequelize.query(
        `update con_cmdb.con_cmdb_configurationitem set con_cmdb_migration_status = :migration_status where con_cmdb_clientid= :clientid AND con_cmdb_unique_id= :unique_id`,
        { replacements: { migration_status: migration_status, clientid: clientid, unique_id: unique_id }, type: Sequelize.QueryTypes.SELECT }
      );
      return res.status(200).send({
        message: response.info_migrationStatusUpdatedSuccessfully,
        success: true,
      });
    }
  };



  static exportCIs = async (req: Request, res: Response) => {
    const ws = fs.createWriteStream("./exportFileStorage/data.csv");

    const { clientid, ci_category, citype, columns, unique_id } = req.body;
    const lang = req.headers.lang ? req.headers.lang : "en";
    const response: any = await getLocalString(lang);

    const columnnames = columns.map((cName: string) => {
      return `con_cmdb_${cName} AS ${cName}`
    });
    let ciCategoryValues = ci_category.length ? ci_category.join("', '") : ""
    let unique_ids = unique_id.length ? unique_id.join("', '") : "";
    let whereclause
    if (Boolean(unique_ids)) {
      whereclause = `AND con_cmdb_unique_id IN('${unique_ids}')`
    } else {
      whereclause = `;`
    }

    console.log(unique_ids);
    logger.info(`exporting CIs for client id: ${clientid}`);
    const selectClause = citype ? `SELECT ${columnnames} FROM con_cmdb.con_cmdb_${citype.toLowerCase()} WHERE con_cmdb_clientid= :clientid AND LOWER(con_cmdb_ci_category) IN(:ciCategoryValue) ${whereclause}` :
      `SELECT ${columnnames} FROM con_cmdb.con_cmdb_configurationitem
        WHERE con_cmdb_clientid= :clientid AND LOWER(con_cmdb_ci_category) IN(:ciCategoryValue) ${whereclause}`

    const cis = await sequelize.query(
      selectClause
      , { replacements: { clientid: clientid, ciCategoryValue: ciCategoryValues.toLowerCase() }, type: Sequelize.QueryTypes.SELECT });
    if (!Boolean(cis.length)) {
      res.status(200).send({
        success: true,
        message: response.info_noDataFoundForProvidedCIType
      });
      logger.info("error");

    } else {
      let data: any = []
      cis.map(function (items: any) {
        const keys = Object.keys(items).map(
          (key) => key.split('_').join('').toUpperCase()
        );
        const values = Object.values(items).map(
          (value) => typeof value == 'object' ? JSON.stringify(value) : typeof value == null ? "" : value
        )
        const attrs = <any>{};
        for (let index = 0; index < keys.length; index++) {
          attrs[keys[index]] = values[index];
        }
        data.push(attrs)
      })
      fastcsv
        .write(data, { headers: true })
        .on("finish", function () {
          var stream = fs.createReadStream("./exportFileStorage/data.csv");
          res.attachment("./exportFileStorage/data.csv");
          stream.pipe(res);
        })
        .pipe(ws);
    }
  };

  static getCIsByUniqueIdAndTypeList = async (
    req: Request,
    res: Response,
    next: NextFunction
  ) => {
    let result = [];
    const { depth, clientid } = req.body;
    const reqData = Object.assign([], req.body.attr);
    logger.info(`fetching CIs By Unique Id And Type List for client id: ${clientid}`);
    for (let index = 0; index < reqData.length; index++) {
      const unique_id = reqData[index].unique_id;
      const ci_category = reqData[index].ci_category;
      const cis = await this.getCIsByUIdAndType(
        unique_id,
        ci_category,
        clientid,
        depth
      );
      result.push(cis);
    }
    return res.status(200).send({
      success: true,
      result,
    });
  };

  static getCIsByUIdAndType = async (
    unique_id: any,
    ci_category: any,
    clientid: any,
    depth: any
  ) => {
    try {
      const configurationItem = <any>(
        await sequelize.query(
          `SELECT * from con_cmdb.con_cmdb_${ci_category} WHERE con_cmdb_unique_id='${unique_id}' AND con_cmdb_clientid='${clientid}' `,
          { type: Sequelize.QueryTypes.SELECT }
        )
      );
      if (configurationItem.length) {
        const attributesMap = [configurationItem].map((attr: any) => {
          const keys = Object.keys(attr[0]).map(
            (key) => `${key.replace(/con_cmdb_/, "")}`
          );
          const values = Object.values(attr[0]).map((val) => val);
          return {
            keys,
            values,
          };
        });
        const keys = attributesMap[0].keys;
        const values = attributesMap[0].values;
        const attrs = <any>{};
        for (let index = 0; index < keys.length; index++) {
          attrs[keys[index]] = values[index];
        }
        const childCIs = [];
        if (depth > 1) {
          const ciType = attrs["ci_category"];
          const relatedwhereClause = `parentci_id= :unique_id and clientid= :clientid`;
          const relatedcis: any = await sequelize.query(
            `
                    SELECT * from con_cmdb.con_cmdb_relatedcis where ${relatedwhereClause}
                `,
            { replacements: { unique_id: unique_id, clientid: clientid }, type: Sequelize.QueryTypes.SELECT }
          );
          if (relatedcis.length > 0) {
            let relationship_name;
            for (let i = 0; i < relatedcis.length; i++) {
              const relatedciguid = relatedcis[i].childci_id;
              relationship_name = relatedcis[i].relationship_name;
              const relatedcitype = relatedcis[i].childci_classname;
              const relatedci = <any>(
                await this.getChildCIs(
                  unique_id,
                  ciType,
                  relatedciguid,
                  relatedcitype,
                  clientid,
                  relationship_name,
                  "parent-to-child",
                  depth - 1
                )
              );
              childCIs.push(relatedci);
            }
          }

          const relatedwhereClause1 = `childci_id= :unique_id and clientid= :clientid`;
          const relatedcis1: any = await sequelize.query(
            `
                    SELECT * from con_cmdb.con_cmdb_relatedcis where ${relatedwhereClause1}
                `,
            { replacements: { unique_id: unique_id, clientid: clientid }, type: Sequelize.QueryTypes.SELECT }
          );
          if (relatedcis1.length > 0) {
            let relationship_name;
            for (let i = 0; i < relatedcis1.length; i++) {
              const relatedciguid = relatedcis1[i].parentci_id;
              relationship_name = relatedcis1[i].relationship_name;
              const relatedcitype = relatedcis1[i].parentci_classname;
              const relatedci: any = <any>(
                await this.getChildCIs(
                  unique_id,
                  ciType,
                  relatedciguid,
                  relatedcitype,
                  clientid,
                  relationship_name,
                  "child-to-parent",
                  depth - 1
                )
              );
              childCIs.push(relatedci);
            }
          }
        }
        const items = {
          //rootci: rootci,
          citype: attrs.ci_category,
          clientid: attrs.clientid,
          attributes: attrs,
          childcis: childCIs,
        };
        return items;
      } else {
        let items = {};
        return items;
      }
    } catch (error: any) {
      logger.error(new Error(error));
      throw error;
    }
  };

  static managedDeviceReportGeneration = async (req: any, res: any) => {
    /* <----- Case 1 : For con_cmdb_ismanagedci=true -----> */

    const category = req.body.category;
    const fromDate: any = req.body.fromDate;
    const toDate: any = req.body.toDate;
    const clientid: any = req.body.clientid;
    let resultArr: any = [];
    let finalArr: any = [];

    const resultForManaged = await sequelize.query(`SELECT con_cmdb_display_name,con_cmdb_unique_id from con_cmdb.con_cmdb_configurationitem where con_cmdb_ismanagedci='true' and con_cmdb_ci_category= :category and con_cmdb_clientid= :clientid`, { replacements: { category: category, clientid: clientid } });
    const ciTableDataForManagedDevice: any = resultForManaged[0];

    for (let j = 0; j < ciTableDataForManagedDevice.length; j++) {
      let count = 0;
      let startDate: any = "";
      let endDate: any = "";
      let notMatchedTrackingObj: any = {};
      const unique_id = ciTableDataForManagedDevice[j].con_cmdb_unique_id;
      const display_name = ciTableDataForManagedDevice[j].con_cmdb_display_name;
      const data = await sequelize.query(`SELECT con_cmdb_unique_id,con_cmdb_display_name,con_cmdb_created_date,con_cmdb_old_managed_status,con_cmdb_new_managed_status from con_cmdb.con_cmdb_manageddevices_tracking where con_cmdb_unique_id= :unique_id and con_cmdb_clientid= :clientid and con_cmdb_created_date >= :fromDate and con_cmdb_created_date <= :toDate order by con_cmdb_created_date`, { replacements: { unique_id: unique_id, clientid: clientid, fromDate: fromDate, toDate: toDate } });
      const trackingTableData: any = data[0];
      if (trackingTableData.length < 1) {
        const difference: any = new Date(moment(toDate).format("YYYY-MM-DD")).valueOf() - new Date(moment(fromDate).format("YYYY-MM-DD")).valueOf();
        const daysDifference = Math.floor(difference / 1000 / 60 / 60 / 24) + 1;
        notMatchedTrackingObj.unique_id = unique_id;
        notMatchedTrackingObj.display_name = display_name;
        notMatchedTrackingObj.diffDays = daysDifference;
        resultArr.push(notMatchedTrackingObj);
      } else if (trackingTableData.length === 1) {
        let resultObj: any = {};
        endDate = toDate;
        if (trackingTableData[0].con_cmdb_new_managed_status === 'managed') {
          startDate = trackingTableData[0].con_cmdb_created_date;
          const difference = new Date(moment(endDate).format("YYYY-MM-DD")).valueOf() - new Date(moment(startDate).format("YYYY-MM-DD")).valueOf();
          const daysDifference = Math.floor(difference / 1000 / 60 / 60 / 24) + 1;
          count = daysDifference;
          resultObj.unique_id = trackingTableData[0].con_cmdb_unique_id;
          resultObj.display_name = trackingTableData[0].con_cmdb_display_name;
          resultObj.diffDays = count;
          resultArr.push(resultObj);
        }
      } else {
        if (trackingTableData[0].con_cmdb_old_managed_status === 'managed') {
          trackingTableData[0].temp_start_date = fromDate;
        }
        if (trackingTableData[trackingTableData.length - 1].con_cmdb_new_managed_status === 'managed') {
          trackingTableData[trackingTableData.length - 1].temp_end_date = toDate;
        }
        trackingTableData.map((elm: any) => {
          let resultObj: any = {};
          if (elm.temp_start_date || elm.temp_end_date) {
            if (elm.temp_start_date) {
              startDate = elm.temp_start_date;
              endDate = elm.con_cmdb_created_date;
            } else {
              endDate = elm.temp_end_date;
              startDate = elm.con_cmdb_created_date;
            }
            const difference = new Date(moment(endDate).format("YYYY-MM-DD")).valueOf() - new Date(moment(startDate).format("YYYY-MM-DD")).valueOf();
            const daysDifference = Math.floor(difference / 1000 / 60 / 60 / 24) + 1;
            count = daysDifference;
            resultObj.unique_id = elm.con_cmdb_unique_id;
            resultObj.display_name = elm.con_cmdb_display_name;
            resultObj.diffDays = count;
            resultArr.push(resultObj);
          } else {
            if (elm.con_cmdb_new_managed_status === 'managed') {
              startDate = elm.con_cmdb_created_date;
            } else {
              endDate = elm.con_cmdb_created_date;
              const difference = new Date(moment(endDate).format("YYYY-MM-DD")).valueOf() - new Date(moment(startDate).format("YYYY-MM-DD")).valueOf();
              const daysDifference = Math.floor(difference / 1000 / 60 / 60 / 24) + 1;
              count = daysDifference;
              resultObj.unique_id = elm.con_cmdb_unique_id;
              resultObj.display_name = elm.con_cmdb_display_name;
              resultObj.diffDays = count;
              resultArr.push(resultObj);
            }
          }
        })
      }
    }

    /* <----- Case 2 : For con_cmdb_ismanagedci=false -----> */

    const resultForLastUnmanaged = await sequelize.query(`SELECT  tracking_table.con_cmdb_display_name,tracking_table.con_cmdb_unique_id,MAX(tracking_table.con_cmdb_created_date) 
    FROM con_cmdb.con_cmdb_manageddevices_tracking as tracking_table
	  inner join con_cmdb.con_cmdb_configurationitem as ci_table 
	  on tracking_table.con_cmdb_unique_id = ci_table.con_cmdb_unique_id
    where con_cmdb_new_managed_status='unmanaged'
	  and con_cmdb_ismanagedci='false'
    and tracking_table.con_cmdb_clientid= :clientid
    group by tracking_table.con_cmdb_display_name,tracking_table.con_cmdb_unique_id
    `, { replacements: { clientid: clientid } });
    const unmanagedDevices: any = resultForLastUnmanaged[0];

    for (let k = 0; k < unmanagedDevices.length; k++) {
      let startDate: any = "";
      let endDate: any = "";
      let count = 0;

      const unique_id = unmanagedDevices[k].con_cmdb_unique_id;
      const data = await sequelize.query(`SELECT con_cmdb_unique_id,con_cmdb_display_name,con_cmdb_created_date,con_cmdb_old_managed_status,con_cmdb_new_managed_status from con_cmdb.con_cmdb_manageddevices_tracking where con_cmdb_unique_id= :unique_id and con_cmdb_clientid= :clientid and con_cmdb_created_date >= :fromDate and con_cmdb_created_date <= :toDate order by con_cmdb_created_date`, { replacements: { unique_id: unique_id, clientid: clientid, fromDate: fromDate, toDate: toDate } });
      const trackingTableData: any = data[0];
      if (trackingTableData.length === 1) {
        let resultObj: any = {};
        startDate = fromDate;
        if (trackingTableData[0].con_cmdb_new_managed_status === 'unmanaged') {
          endDate = trackingTableData[0].con_cmdb_created_date;
          const difference = new Date(moment(endDate).format("YYYY-MM-DD")).valueOf() - new Date(moment(startDate).format("YYYY-MM-DD")).valueOf();
          const daysDifference = Math.floor(difference / 1000 / 60 / 60 / 24) + 1;
          count = daysDifference;
          resultObj.unique_id = trackingTableData[0].con_cmdb_unique_id;
          resultObj.display_name = trackingTableData[0].con_cmdb_display_name;
          resultObj.diffDays = count;
          resultArr.push(resultObj);
        }
      }
      if (trackingTableData.length > 1) {
        if (trackingTableData[0].con_cmdb_old_managed_status === 'managed') {
          trackingTableData[0].temp_start_date = fromDate;
        }
        if (trackingTableData[trackingTableData.length - 1].con_cmdb_new_managed_status === 'managed') {
          trackingTableData[trackingTableData.length - 1].temp_end_date = toDate;
        }
        trackingTableData.map((elm: any) => {
          let resultObj: any = {};
          if (elm.temp_start_date || elm.temp_end_date) {
            if (elm.temp_start_date) {
              startDate = elm.temp_start_date;
              endDate = elm.con_cmdb_created_date;
            } else {
              endDate = elm.temp_end_date;
              startDate = elm.con_cmdb_created_date;
            }
            const difference = new Date(moment(endDate).format("YYYY-MM-DD")).valueOf() - new Date(moment(startDate).format("YYYY-MM-DD")).valueOf();
            const daysDifference = Math.floor(difference / 1000 / 60 / 60 / 24) + 1;
            count = daysDifference;
            resultObj.unique_id = elm.con_cmdb_unique_id;
            resultObj.display_name = elm.con_cmdb_display_name;
            resultObj.diffDays = count;
            resultArr.push(resultObj);
          } else {
            if (elm.con_cmdb_new_managed_status === 'managed') {
              startDate = elm.con_cmdb_created_date;
            } else {
              endDate = elm.con_cmdb_created_date;
              const difference = new Date(moment(endDate).format("YYYY-MM-DD")).valueOf() - new Date(moment(startDate).format("YYYY-MM-DD")).valueOf();
              const daysDifference = Math.floor(difference / 1000 / 60 / 60 / 24) + 1;
              count = daysDifference;
              resultObj.unique_id = elm.con_cmdb_unique_id;
              resultObj.display_name = elm.con_cmdb_display_name;
              resultObj.diffDays = count;
              resultArr.push(resultObj);
            }
          }
        })
      }
    }

    /* <----- Clubbing both the result to make a single array -----> */

    const finalResult: any = Array.from(resultArr.reduce(
      (m: { set: (arg0: any, arg1: any) => any; get: (arg0: any) => any; }, { unique_id, diffDays }: any) => m.set(unique_id, (m.get(unique_id) || 0) + diffDays), new Map
    ), ([unique_id, diffDays]) => ({ unique_id, diffDays }));

    for (let p = 0; p < finalResult.length; p++) {
      let finalObj: any = {};
      const unique_id = finalResult[p].unique_id;
      const diffDays = finalResult[p].diffDays;
      const data = await sequelize.query(`SELECT con_cmdb_display_name,con_cmdb_unique_id from con_cmdb.con_cmdb_configurationitem where con_cmdb_unique_id= :unique_id and con_cmdb_clientid= :clientid`, { replacements: { unique_id: unique_id, clientid: clientid } });
      const requiredTrackingTableData: any = data[0];

      requiredTrackingTableData.map((item: any) => {
        finalObj.unique_id = item.con_cmdb_unique_id;
        finalObj.display_name = item.con_cmdb_display_name;
        finalObj.diffDays = diffDays;
      });
      finalArr.push(finalObj);
    }
    res.send(finalArr);
  }

  static getChildCIs = async (
    rootUnique_id: any,
    rootCIType: any,
    unique_id: any,
    ci_category: any,
    clientid: any,
    ci_relationship_name: any,
    ci_relationship_direction: any,
    depth: any
  ) => {
    try {
      const configurationItem = <any>await sequelize.query(
        `SELECT * from con_cmdb.con_cmdb_${ci_category} WHERE con_cmdb_unique_id='${unique_id}' AND con_cmdb_clientid='${clientid}'
            `,
        { type: Sequelize.QueryTypes.SELECT }
      );
      if (configurationItem.length) {
        const attributesMap = [configurationItem].map((attr: any) => {
          const keys = Object.keys(attr[0]).map(
            (key) => `${key.replace(/con_cmdb_/, "")}`
          );
          const values = Object.values(attr[0]).map((val) => val);
          return {
            keys,
            values,
          };
        });
        const keys = attributesMap[0].keys;
        const values = attributesMap[0].values;
        const attrs = <any>{};
        for (let index = 0; index < keys.length; index++) {
          attrs[keys[index]] = values[index];
        }
        const childCIs = [];

        if (depth > 1) {
          const citype = attrs["ci_category"];

          const relatedwhereClause = `(parentci_id='${unique_id}' and childci_id != '${rootUnique_id}') and clientid='${clientid}' and childci_classname != '${rootCIType}'`;
          const relatedcis: any = await sequelize.query(
            `
            SELECT * from con_cmdb.con_cmdb_relatedcis where ${relatedwhereClause}
        `,
            { type: Sequelize.QueryTypes.SELECT }
          );
          if (relatedcis.length > 0) {
            let relationship_name;
            for (let i = 0; i < relatedcis.length; i++) {
              const relatedciguid = relatedcis[i].childci_id;
              relationship_name = relatedcis[i].relationship_name;
              const relatedcitype = relatedcis[i].childci_classname;
              //const childci_classname = relatedcis[i].childci_classname
              // if (childci_classname != parentCIType) {
              const relatedci = <any>(
                await this.getChildCIs(
                  unique_id,
                  citype,
                  relatedciguid,
                  relatedcitype,
                  clientid,
                  relationship_name,
                  "parent-to-child",
                  depth - 1
                )
              );
              childCIs.push(relatedci);
              // }
            }
          }
          const relatedwhereClause1 = `(childci_id='${unique_id}' and parentci_id != '${rootUnique_id}') and clientid='${clientid}' and parentci_classname != '${rootCIType}'`;
          const relatedcis1: any = await sequelize.query(
            `
                    SELECT * from con_cmdb.con_cmdb_relatedcis where ${relatedwhereClause1}
                `,
            { type: Sequelize.QueryTypes.SELECT }
          );
          if (relatedcis1.length > 0) {
            let relationship_name;
            for (let i = 0; i < relatedcis1.length; i++) {
              const relatedciguid = relatedcis1[i].parentci_id;
              relationship_name = relatedcis1[i].relationship_name;
              const relatedcitype = relatedcis1[i].parentci_classname;
              //const parentci_classname = relatedcis1[i].parentci_classname
              // if (parentci_classname != parentCIType) {
              const relatedci = <any>(
                await this.getChildCIs(
                  unique_id,
                  citype,
                  relatedciguid,
                  relatedcitype,
                  clientid,
                  relationship_name,
                  "child-to-parent",
                  depth - 1
                )
              );
              childCIs.push(relatedci);
              // }
            }
          }
        }
        const items: any = {
          citype: attrs.ci_category,
          relationship_direction: ci_relationship_direction,
          relationship: ci_relationship_name,
          attributes: attrs,
          childcis: childCIs,
        };
        return items;
      } else {
        let items = {};
        return items;
      }
    } catch (error: any) {
      logger.error(new Error(error));
      throw error;
    }
  };



  static automateCmdbConfiguration = async (req: Request,
    res: Response,
    next: NextFunction) => {
    let auditRequest: any = {}
    if (auditLogsEnabled === 'true') {
      auditRequest = {
        client_id: req.headers.clientid,
        service_name: "cmdb-ciclass-manager",
        module: "CMDB-automation",
        db_table: "CMDB-CONFIGURATIONS",
        description: "CMDB Automation Completed Successfully",
        post_audit_data: req.body,
        action: "Trigger",
        performed_by: req.headers.username,
        action_date: actionDate.format('YYYY-MM-DD'),
        user_id: req.headers.userid,
        realm: req.headers?.realm,
        apitoken: req.headers?.apitoken
      }
    }
    let { created_by, identification_rules, relationship, ui_configuration, classes } = req.body;
    const lang = req.headers.lang ? req.headers.lang : "en";
    const response: any = await getLocalString(lang);
    let { clientid } = req.headers;
    if ((req as any).user) {
      const user: any = (req as any).user;
      created_by = user.userName;
    }
    let flag = 1
    let result = []
    let data = ""
    logger.info("starting automations")
    if (classes == 'classes and attributes') {
      let class_seeder: any = await CMDBDataSeedController.seedClasses()
      logger.info("after loading classes :%s", class_seeder.message)
      if (class_seeder.message == "Classes loaded successfully,please load class attributes") {
        data = "classes loaded successfully"
        result.push(data)
        let attribute_seeder: any = await CMDBDataSeedController.seedAttributes()
        logger.info("after loading attributes :%s", attribute_seeder.message)
        if (attribute_seeder.message == "Attributes loaded successfully, please perfom cmdb sync") {
          data = "attributes loaded successfully"
          result.push(data)
          const sync = await CIClasscontroller.cmdbSync1(created_by, response)
          logger.info("after completing the sync :%s", sync.message)
          if (sync.message == "Everything is Synced Sucessfully") {
            flag = 2
            data = "classes & attributes synced successfully"
            result.push(data)
          } else {
            return res.status(200).send({
              message: sync.message,
              sucess: true
            })
          }

        } else {
          return res.status(200).send({
            message: attribute_seeder.message,
            sucess: true
          })
        }
      } else {
        return res.status(200).send({
          message: class_seeder.message,
          success: true
        })
      }

    } else {
      const cmdb_classes = await sequelize.query(`select * from public.cmdb_classes`, { type: Sequelize.QueryTypes.SELECT })
      logger.info(cmdb_classes.length, ">>>>")
      if (Boolean(cmdb_classes.length)) {
        flag = 2
      }
    }
    if (flag == 2) {
      if (relationship == 'relationship') {
        const relationships: any = await CMDBRelationshipsController.relationship(automation.relationships, response, auditRequest)
        result.push(relationships.message)

      }
      if (identification_rules == 'identification_rules') {
        const identification: any = await CIIdentificationController.identificationImport(automation.identificaion_rules, response, auditRequest)
        result.push(identification.message)
      }
      if (ui_configuration == 'ui_configuration') {
        const UIconfiguration = await UIConfigurationController.importUIconfigurationsMethod(automation.UIconfiguration, clientid, response, auditRequest)
        result.push(UIconfiguration.message)
      }
    } else {
      return res.status(200).send({
        message: response.info_classesAndAttributesAreNotLoaded,
        success: true
      })
    }
    if (auditLogsEnabled === 'true') {
      await auditLogsController.createauditDetails(auditRequest);
    }
    return res.status(200).send({
      result,
      message: response.info_automationCompletedSuccessfully,
      success: true
    })

  }

  static getCIsByQuery = async (req: Request, res: Response, next: NextFunction) => {
    try {
      let parentWhereClause = "";
      let childWhereClause = ""
      let relatedWhereClause = ""
      let joinClause1 = ""
      let joinClause2 = ""
      let attributelist: any = []
      let finalSQLQuery = ""
      const MIN_SIZE = 10;
      const defaultpagination = true;
      let countclause = ""

      const allColumns = ['p.con_cmdb_display_name', 'p.con_cmdb_private_ip'];

      const searchClause = (q: string) => {
        return allColumns.map((data, index, array) => {
          if (index === array.length - 1) {
            return `${data} ILIKE '%${q}%'`
          } else {
            return `${data} ILIKE '%${q}%' OR`
          }
        }).join(' ');
      }

      const columnNamesAs = (columnName: string[]) => {
        return columnName.map((cName: string) => {
          return `${cName} AS ${cName.replace(/p.con_cmdb_/, '')}`;
        });
      };

      const childcolumnNamesAs = (columnName: string[]) => {
        return columnName.map((cName: string) => {
          return `${cName} AS ${cName.replace(/c.con_cmdb_/, 'child_')}`;
        });
      };

      const { clientid, citype, size = MIN_SIZE, depth = 1, page = 1, pagination = defaultpagination, q } = req.body;
      const query_attrs = req.body.query_attrs;
      const resultattrs = req.body.result_attrs
      const parentattrList = resultattrs.split(',').map((attr: string) => `p.con_cmdb_${attr}`)
      const resultattrList = columnNamesAs(parentattrList)

      parentWhereClause = `p.con_cmdb_clientid='${clientid}'`
      //if query attrs is undefined or null or notempty then processess below
      if (query_attrs != undefined && query_attrs !== null) {
        const query_expression = this.queryProcessor(query_attrs, 'p')
        logger.info("query expression==>", query_expression)
        parentWhereClause = `${parentWhereClause} and (${query_expression})`
        logger.info(" parentWhereClause:", parentWhereClause)
      }
      const childcis = req.body.childcis
      logger.info(childcis, "...")
      let childCIExist = false
      // if (childcis is not undefined or not null) {  //condition to be corrected
      if (childcis !== undefined && childcis !== null) {
        logger.info(",,,,,,,,,,,,")
        childCIExist = true
        const childci = childcis[0]
        const childcitype = childci.citype
        const childresultattrs = childci.result_attrs
        const childresultAttrList = childcolumnNamesAs(childresultattrs.split(',').map((attr: string) => `c.con_cmdb_${attr}`))
        logger.info("childresultAttrList:", childresultAttrList, ";;;;;")

        // const combinedattrs = [...parentattrList, ...childresultAttrList]
        attributelist = [...resultattrList, ...childresultAttrList]

        if (childci.query_attrs != undefined && childci.query_attrs !== null) {
          const child_query_expression = this.queryProcessor(childci.query_attrs, 'c')
          logger.info("child query expression==>", child_query_expression)
          // childWhereClause = `(${child_query_expression})`
          childWhereClause = `${child_query_expression}`
        }

        const relationship_name = childci.relationship_name
        const relationship_direction = childci.relationship_direction
        relatedWhereClause = `r.clientid='${clientid}' and r.relationship_name='${relationship_name}'`

        if (relationship_direction == "parent-child") {
          joinClause1 = `inner join con_cmdb.con_cmdb_relatedcis as r on p.con_cmdb_unique_id = r.parentci_id`
          joinClause2 = `inner join con_cmdb.con_cmdb_${childcitype} as c on r.childci_id = c.con_cmdb_unique_id`
        }
        else if (relationship_direction == "child-parent") {
          joinClause1 = `inner join con_cmdb.con_cmdb_relatedcis as r on p.con_cmdb_unique_id = r.childci_id`
          joinClause2 = `inner join con_cmdb.con_cmdb_${childcitype} as c on r.parentci_id = c.con_cmdb_unique_id`
        }
      }
      if (childCIExist) {
        logger.info(attributelist, "attrList<===")
        if (pagination == true) {
          finalSQLQuery = `select ${attributelist} from con_cmdb.con_cmdb_${citype} as p ${joinClause1} ${joinClause2} where ${parentWhereClause} and ${relatedWhereClause} and ${childWhereClause}
            LIMIT :size 
            OFFSET (:page - 1) * :size`

        } else {
          finalSQLQuery = `select ${attributelist} from con_cmdb.con_cmdb_${citype} as p ${joinClause1} ${joinClause2} where ${parentWhereClause} and ${relatedWhereClause} and ${childWhereClause}`
        }
        countclause = `select count(*) from con_cmdb.con_cmdb_${citype} as p ${joinClause1} ${joinClause2} where ${parentWhereClause} and ${relatedWhereClause} and ${childWhereClause}`

      }

      else {
        let whereClause = q ? `${parentWhereClause} and (${searchClause(q)})` : parentWhereClause
        if (pagination == true) {
          finalSQLQuery = `select ${resultattrList} from con_cmdb.con_cmdb_${citype} as p where ${whereClause}
            LIMIT :size 
            OFFSET (:page - 1) * :size`
        } else {
          finalSQLQuery = `select ${resultattrList} from con_cmdb.con_cmdb_${citype} as p where ${whereClause}`
        }
        countclause = `select count(*) from con_cmdb.con_cmdb_${citype} as p where ${whereClause}`
      }

      logger.info(finalSQLQuery, ">>>>")

      const ciList: any = await sequelize.query(finalSQLQuery, {
        replacements: { page: page, size: size },
        type: Sequelize.QueryTypes.SELECT
      });

      const [{ count }] = <any>await sequelize.query(countclause, {
        type: Sequelize.QueryTypes.SELECT
      });

      const pages = Math.ceil(count / (size ? Number(size) : MIN_SIZE));

      return res.status(200).send({
        items: ciList,
        count,
        pages,
        page: Number(page),
        success: true
      });
    } catch (error: any) {
      logger.error(new Error(error));
      next(error);
    }
  };

  static bulkStatusUpdate = async (req: Request, res: Response, next: NextFunction) => {
    try {
      let auditRequest: any = {}
      if (auditLogsEnabled === 'true') {
        auditRequest = {
          client_id: req.headers.clientid,
          service_name: "cmdb-ciclass-manager",
          module: "configuration-item",
          db_table: "con_cmdb_configurationitem",
          description: "CIs Status updated successfully",
          post_audit_data: req.body,
          action: "Update",
          performed_by: req.headers.username,
          action_date: actionDate.format('YYYY-MM-DD'),
          user_id: req.headers.userid,
          realm: req.headers?.realm,
          apitoken: req.headers?.apitoken
        }
      }
      const { ids, cistatus } = req.body;
      const lang = req.headers.lang ? req.headers.lang : "en";
      const response: any = await getLocalString(lang);
      for (let index = 0; index < ids.length; index++) {
        const element = ids[index];
        const updateQuery = await sequelize.query(`update con_cmdb.con_cmdb_configurationitem set con_cmdb_cistatus = :cistatus where con_cmdb_unique_id = :element`, {
          replacements: { cistatus: cistatus, element: element },
          type: Sequelize.QueryTypes.UPDATE
        });
      }
      if (auditLogsEnabled === 'true') {
        auditRequest.pre_audit_data = ids
        await auditLogsController.createauditDetails(auditRequest);
      }
      return res.status(200).send({
        message: response.info_CIStatusUpdatedSuccessfully,
        success: true
      })
    } catch (error: any) {
      logger.error(new Error(error));
      next(error);

    }
  }

  static getInstalledSoftwares = async (
    req: Request,
    res: Response,
    next: NextFunction
  ) => {
    try {
      const { unique_id, clientid, raw = false } = req.body;
      const lang = req.headers.lang ? req.headers.lang : "en";
      const response: any = await getLocalString(lang);
      if (!Boolean(clientid)) return next(new Error(response.info_clientidRequired));
      if (!Boolean(unique_id)) return next(new Error(response.info_uniqueIDRequired));
      logger.info(`fetch CI By Id from db for client id: ${clientid}`);

      const result = [];

      if (raw == true) {

        const whereClause = `unique_id = :unique_id and client_id= :clientid`
        const query = await sequelize.query(
          `
                    SELECT * from con_cmdb.con_cmdb_installed_packages where ${whereClause}
                `,
          { replacements: { unique_id: unique_id, clientid: clientid }, type: Sequelize.QueryTypes.SELECT }
        );
        if (query.length > 0) {
          result.push(...query)
        }
        else {
          result.push({})
        }
      } else {
        const relatedwhereClause1 = `childci_id= :unique_id and clientid= :clientid and relationship_name ='installedOn'`;

        const relatedcis1: any = await sequelize.query(
          `
                    SELECT * from con_cmdb.con_cmdb_relatedcis where ${relatedwhereClause1}
                `,
          { replacements: { unique_id: unique_id, clientid: clientid }, type: Sequelize.QueryTypes.SELECT }
        );

        if (relatedcis1.length > 0) {
          let relationship_name;
          //for loop through i = 0 to length {
          for (let i = 0; i < relatedcis1.length; i++) {
            const relatedciguid = relatedcis1[i].parentci_id;
            relationship_name = relatedcis1[i].relationship_name;
            const relatedciCategory = `con_cmdb_${relatedcis1[i].parentci_classname}`;
            // if (!db_table_names.includes(relatedciCategory.toLowerCase())) {
            //   return next(new Error('Dbname not exists in the database'))
            // }
            const relatedci = <any>await sequelize.query(
              `SELECT * from con_cmdb."${relatedciCategory.toLowerCase()}" WHERE con_cmdb_unique_id= :relatedciguid AND con_cmdb_clientid= :clientid
                            `,
              { replacements: { relatedciguid: relatedciguid, clientid: clientid }, type: Sequelize.QueryTypes.SELECT }
            );
            if (relatedci.length > 0) {
              const relatedciattributesMap = [relatedci].map((attr: any) => {
                const keys = Object.keys(attr[0]).map(
                  (key) => `${key.replace(/con_cmdb_/, "")}`
                );
                const values = Object.values(attr[0]).map((val) => val);
                return {
                  keys,
                  values,
                };
              });

              const keys = relatedciattributesMap[0].keys;
              const values = relatedciattributesMap[0].values;
              const relatedattrs = <any>{};
              for (let index = 0; index < keys.length; index++) {
                relatedattrs[keys[index]] = values[index];
              }

              result.push(relatedattrs); //add childcitem to child cis list
            }
          }
        }
      }

      const items = {
        installed_software: result,
      };
      // delete items.attributes.ci_category;
      // delete items.attributes.clientid;

      return res.status(200).send({
        message: response.info_installedSoftwaresFetchedSuccessfully,
        success: true,
        items,
      });
    } catch (error) {
      logger.error(new Error(`Error from getInstalledSoftware ${error}`));
      next(error);
    }
  };

  static createMultipleCIs = async (
    req: Request,
    res: Response,
    next: NextFunction
  ) => {
    const { ci_payload } = req.body
    const lang = req.headers.lang ? req.headers.lang : "en";
    const response: any = await getLocalString(lang);
    try {
      let final_result = []
      let ci_result = {}
      for (let index = 0; index < ci_payload.length; index++) {
        const clientId = ci_payload[index].clientid;
        const toplevelClasstype = ci_payload[index].citype;
        const created_by = ci_payload[index].attributes.created_by;
        const ip_address = ci_payload[index].attributes.private_ip

        //get the identification rules matching CItype, clientid, datasource
        // identificationwhereclouse = CItype

        let _items = {};
        let result: any = [];

        try {
          logger.info("creating new CIs into db...");
          const items =
            await CIIdentificationController._getCIIdentificationByIdForCIs(
              clientId,
              toplevelClasstype
            );
          if (items === false) {
            return next("Not Found");
          }

          _items = items;
          logger.info(`${_items}>>>>>>>>>>>>`);
        } catch (error: any) {
          logger.error(new Error(error));
          return next(error);
        }

        const identificationDef: any = _items;
        let toplevelCIUUID = null;
        let ciExists;

        if (identificationDef.length > 0) {
          // toplevelCIUUID = await this.createOrUpdateCI(toplevelClasstype, req.body.attributes, clientId, identificationDef[0]);
          const topCIResult = await this.createOrUpdateCI(
            toplevelClasstype,
            ci_payload[index].attributes,
            clientId,
            identificationDef[0]
          );

          toplevelCIUUID = topCIResult.ciUUID;
          ciExists = topCIResult.ciExists;

          if (toplevelCIUUID === null || toplevelCIUUID === "") {
            //conditionn needs to be pushed as child cis
            ci_result = {
              message: response.info_ciNotCreatedOrUpdated,
              success: false,
            };

          } else {

            logger.info(
              `toplevelCIUUID =>> ${topCIResult}, ${toplevelClasstype}, ${ci_payload[index].attributes}, ${clientId}, ${identificationDef[0]}, ${toplevelCIUUID}`
            );
            const childCIs = ci_payload[index].childcis;
            // const parentCIUUID = toplevelCIUUID;
            // const parentcitype = toplevelClasstype;
            let ci_uuid_mapping: any = new Map();
            ci_uuid_mapping.set(0, {
              uuid: toplevelCIUUID,
              citype: toplevelClasstype,
            });
            //if input payload contains key called additional_tables
            //then call insertAdditionalTables method(new method)-->(ci_uuid_mapping,additional_tables)
            const entry = ci_uuid_mapping.get(0);
            const additional_data = ci_payload[index].additional_tables
            const ipaddress = ip_address ? ip_address : null

            if (additional_data) {
              await this.insertAdditionalTables(additional_data, entry.uuid, entry.citype, ipaddress, clientId)
            }
            if (childCIs) {
              logger.info(`CHILD_CI : ${childCIs}`);
              for (let index = 0; index < childCIs.length; index++) {
                logger.info(`CHILD_CI inside loop: ${childCIs[index]}`);
                const childCI = childCIs[index];
                if (
                  childCI.relationship !== undefined &&
                  childCI.relationship.length > 0
                ) {
                  const childCItype = childCI.citype;
                  logger.info(`childCItype: ${childCItype}`);
                  logger.info(`CLI: ${clientId}`);

                  const direction = childCI.relationship_direction;

                  const items =
                    await CIIdentificationController._getCIIdentificationByIdForCIs(
                      clientId,
                      childCItype
                    );
                  if (items === false) {
                    return next("Not Found");
                  }
                  const childIdentificationDef = items;
                  logger.info(
                    `childIdentificationDef==================> ${items} =========>`
                  );
                  if (childIdentificationDef.length > 0) {
                    // const childCIUUID = await this.createOrUpdateCI(childCItype, childCI.attributes, clientId, childIdentificationDef[0]);
                    const childResult = await this.createOrUpdateCI(
                      childCItype,
                      childCI.attributes,
                      clientId,
                      childIdentificationDef[0]
                    );
                    const childCIUUID = childResult.ciUUID;
                    if (childCIUUID === null || childCIUUID === "") {
                      result.push({
                        message: response.info_childCINotCreatedOrUpdated,
                        success: false,
                      });
                    } else {
                      ci_uuid_mapping.set(childCI.current_mapping_level, {
                        uuid: childCIUUID,
                        citype: childCItype,
                      });

                      result.push({
                        message: `${ciExists
                          ? response.info_childCIUpdatedSuccessfully
                          : response.info_childCICreatedSuccessfully
                          }`,
                        success: true,
                        unique_id: childCIUUID,
                        ci_name: childCI.attributes.display_name,
                        ci_type: childCI.citype,
                      });
                      let parentcitype = "";
                      let parentCIUUID = "";
                      if (
                        !Boolean(childCI.parent_mapping_level) ||
                        childCI.parent_mapping_level == null
                      ) {
                        parentCIUUID = ci_uuid_mapping.get(0).uuid;
                        parentcitype = ci_uuid_mapping.get(0).citype;
                      } else {
                        parentCIUUID = ci_uuid_mapping.get(
                          childCI.parent_mapping_level
                        ).uuid;
                        parentcitype = ci_uuid_mapping.get(
                          childCI.parent_mapping_level
                        ).citype;
                        logger.info(`parentcitype: ${parentcitype}`);
                      }

                      if (direction === "parent-to-child") {
                        this.createOrUpdateRelationship(
                          parentCIUUID,
                          parentcitype,
                          childCI.relationship,
                          childCIUUID,
                          childCItype,
                          clientId,
                          created_by
                        );
                      }
                      if (direction === "child-to-parent") {
                        this.createOrUpdateRelationship(
                          childCIUUID,
                          childCItype,
                          childCI.relationship,
                          parentCIUUID,
                          parentcitype,
                          clientId,
                          created_by
                        );
                      }
                    }
                  }
                }
              }
            }

            ci_result = {
              message: `${ciExists ? response.info_ciUpdatedSuccessfully : response.info_ciCreatedSuccessfully
                }`,
              success: true,
              unique_id: toplevelCIUUID,
              ci_name: ci_payload[index].attributes.display_name,
              ci_type: ci_payload[index].citype,
              child_cis: result,
            };
          }
          logger.info(`${toplevelCIUUID}, ${identificationDef.length} >>>>>>>`);


        }
        final_result.push(ci_result)
      }
      return res.status(200).send(final_result)

    } catch (error: any) {
      logger.error(new Error(error));
      next(error);
    }
  };
}

export default CIsController;

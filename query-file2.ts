 static getCIBaslinesHistory = async (req: Request, res: Response, next: NextFunction) => {
    const { baseline_name, citype, unique_id } = req.body;
    const lang = req.headers.lang ? req.headers.lang : "en";
    const response: any = await getLocalString(lang);
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
      "con_cmdb_cistatus",
      "con_cmdb_last_discovered_time",
      "con_cmdb_discovery_runidentifier",
      "con_cmdb_ci_operation",
      "con_cmdb_ct_created",
    ];
    logger.info("fetching CI Baslines History from db...");
    const table_name = `ct_con_cmdb_${citype}`
    const queryInterface = sequelize.getQueryInterface();
    const tableDescription = await queryInterface.describeTable(table_name.toLowerCase(), { schema: 'con_cmdb' }).catch((err => {
      return next(new Error('Table does not exist or invalid table name'));
    }))
    if (tableDescription) {
      const baselines: any = await sequelize.query(
        `SELECT * FROM con_cmdb.${table_name}
       WHERE con_cmdb_baseline_name = :baseline_name 
       AND con_cmdb_unique_id = :unique_id 
       ORDER BY con_cmdb_last_modified_time`,
        {
          replacements: { baseline_name: baseline_name, unique_id: unique_id },
          type: Sequelize.QueryTypes.SELECT
        }
      );
      let result = [];
      let time;

      for (let i = 1; i < baselines.length; i++) {
        for (let j = i - 1; j < i; j++) {
          let x = Object.keys(baselines[j]);
          let y = Object.values(baselines[j]);
          let keys = Object.keys(baselines[i]);
          let values = Object.values(baselines[i]);
          for (let index = 0; index < keys.length; index++) {
            if (keys[index] == "con_cmdb_last_modified_time") {
              time = values[index];
            }
          }
          for (let k = 0; k < keys.length; k++) {
            if (attr_exclusion_list.indexOf(keys[k]) === -1) {
              if (values[k] !== y[k]) {
                result.push({
                  attributename: `${keys[k].replace(/con_cmdb_/, "")}`,
                  oldvalue: y[k],
                  newvalue: values[k],
                  time: time,
                });
              }
            }
          }
        }
      }
      logger.info("SUCCESS GET CI Baseline History: ");
      return res.status(200).send({
        message: response.info_baselinesFetchedSuccessfully,
        success: true,
        items: result,
      });
    }
  };
  
   static getDiscoveryCIsByMultipleTypes = async (req: Request, res: Response, next: NextFunction) => {

        try {
            const lang = req.headers.lang ? req.headers.lang : "en";
            const response: any = await getLocalString(lang);
            const MIN_SIZE = 10;
            const { q, clientid, depth = 1, size = MIN_SIZE, page = 1, filter_attrs, order = 'DESC', column_name = 'last_discovered_time', discovery_name } = req.body;
            let ciCategoryValues = req.body.ci_category.length ? req.body.ci_category : [""]

            if (size < 1 || page < 1) return next(new Error(response.info_pageSizeMustNotNegative));
            if (!Boolean(clientid)) return next(new Error(response.info_clientidRequired));
            let CIList: any = await sequelize.query(
                `SELECT unique_id FROM discovery."discovery_ci_mapping"
                WHERE client_id= :clientid AND discovery_name= :discovery_name`
                , { replacements: { clientid: clientid, discovery_name: discovery_name }, type: Sequelize.QueryTypes.SELECT });
            if (CIList.length) {
                let uniqueIDList: any = CIList.map((value: any) => value.unique_id)
                let uniqueList: any = uniqueIDList.length ? uniqueIDList : [""]
                if ((!req.body.ci_category.length)) return next(new Error(response.info_ci_categoryRequired));
                const sortingQuery = (Boolean(filter_attrs) && Boolean(Object.keys(filter_attrs).length)) ? this.filterQuery(filter_attrs) : '';
                const columnNamesAs = (columnName: string[]) => {
                    return columnName.map((cName: string) => {
                        return `${cName} AS ${cName.replace(/con_cmdb_/, '')}`;
                    });
                };

                const allColumns = ['con_cmdb_display_name', 'con_cmdb_ci_category', 'con_cmdb_source', 'con_cmdb_unique_id'];

                const searchClause = (searchTerm: string) => {
                    return {
                        [Sequelize.Op.or]: allColumns.map((column) => ({
                            [column]: {
                                [Sequelize.Op.iLike]: `%${searchTerm}%`
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
                let whereClause = {
                    [Sequelize.Op.and]: [
                        { con_cmdb_clientid: clientid, con_cmdb_ci_category: ciCategoryValues.map((v: string) => v.toLowerCase()), con_cmdb_unique_id: uniqueList }
                    ]
                };
                if (Boolean(q)) {
                    whereClause[Sequelize.Op.and].push(searchClause(q) as any)
                }
                const queryGenerator = (sequelize.getQueryInterface() as any).queryGenerator;
                const selectClause = q ?
                    `SELECT ${CIsColumnName} FROM con_cmdb.con_cmdb_configurationitem
                        WHERE ${queryGenerator.getWhereConditions(whereClause)} 
                        ${sortingQuery}
                        ORDER BY con_cmdb_${column_name} ${order}
                        LIMIT :size 
                        OFFSET (:page - 1) * :size`
                    :
                    `SELECT ${CIsColumnName} FROM con_cmdb.con_cmdb_configurationitem
                        WHERE ${queryGenerator.getWhereConditions(whereClause)} 
                        ${sortingQuery}
                        ORDER BY con_cmdb_${column_name} ${order}
                        LIMIT :size 
                        OFFSET (:page - 1) * :size`;

                const replacements = {
                    size: size,                               // Limit for pagination
                    page: page                                // Page number for pagination
                };
                const items = await sequelize.query(
                    selectClause
                    , { replacements: replacements, type: Sequelize.QueryTypes.SELECT });

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
    type: Sequelize.QueryTypes.SELECT,
    replacements: { clientid: clientid },
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

            } else {
                return res.status(200).send({
                    message: response.info_dataNotFound,
                    success: true,
                });
            }
        }
        catch (error) {
            logger.error(new Error(`Error from getAllCIs ${error}`));
            next(error);
        }
    };

import uuid
import logging
from sqlalchemy import or_, select, insert
from concurrent.futures import ThreadPoolExecutor, as_completed
from .base_migrator import BaseMigrator

logger = logging.getLogger(__name__)

class InstitutionMigrator(BaseMigrator):
    def __init__(self, engine, source_engine, dest_engine, storage, config):
        super().__init__(engine, source_engine, dest_engine, storage)
        self.config = config

    def migrate(self) -> int:
        logger.info("Starting Institution Migration...")
        
        with self.dest_engine.begin() as dest_conn:
            # Step 1: Migrate Main Institutions (parent_id is NULL)
            count_main = self._migrate_main_institutions(dest_conn)
            
            # Step 2: Migrate Campuses (parent_id is NOT NULL)
            count_campuses = self._migrate_campuses(dest_conn)
            
            # Step 3: Update existing institutions in target db
            count_updates = self._update_existing_institutions(dest_conn)
            
            return count_main + count_campuses + count_updates

    def _migrate_main_institutions(self, dest_conn) -> int:
        logger.info("Migrating Main Institutions...")
        
        # Manually reflect tables to avoid FK issues
        institution_table = self._manual_reflect('institution', self.source_engine, self.metadata_source)
        address_table = self._manual_reflect('address', self.source_engine, self.metadata_source)
        state_table = self._manual_reflect('state', self.source_engine, self.metadata_source)
        country_table = self._manual_reflect('country', self.source_engine, self.metadata_source)
        dest_table = self._manual_reflect('institutions', self.dest_engine, self.metadata_dest)
        
        if not dest_table.columns:
            logger.error("Destination table 'institutions' not found or has no columns.")
            raise ValueError("Destination table 'institutions' not found.")

        parent_institution_column = self._parent_institution_column(
            institution_table
        )

        # Build join query for parent institution rows. Self-parent rows
        # are inserted as both institutions and campuses.
        allowed_types = ['university', 'employer', 'parentuniversity', 'serviceprovider', 'regionalServiceProvider']
        query = select(institution_table, address_table, state_table, country_table).select_from(
            institution_table
            .join(address_table, institution_table.c.address_id == address_table.c.id, isouter=True)
            .join(state_table, address_table.c.state == state_table.c.id, isouter=True)
            .join(country_table, address_table.c.country == country_table.c.id, isouter=True)
        ).where(
            self._is_main_institution_condition(
                institution_table,
                parent_institution_column
            ),
            institution_table.c.institution_type.in_(allowed_types)
        )

        if self.config.get('limit'):
            query = query.limit(self.config['limit'])

        with self.source_engine.connect() as source_conn:
            results = source_conn.execute(query)
            
            rows = results.all()
            if not rows:
                return 0

            logger.info(f"Processing {len(rows)} institutions and their images...")
            
            # Prepare upload tasks
            upload_tasks = []
            with ThreadPoolExecutor(max_workers=10) as executor:
                for row in rows:
                    row_dict = row._mapping
                    new_uuid = str(uuid.uuid4())
                    inst_id = row_dict[institution_table.c.id]
                    self.engine.id_map[inst_id] = new_uuid
                    
                    # Submit upload tasks
                    seal_future = executor.submit(
                        self.storage.upload_base64, 
                        row_dict[institution_table.c.institution_seal], 
                        f"seals/{new_uuid}.png"
                    )
                    logo_future = executor.submit(
                        self.storage.upload_base64, 
                        row_dict[institution_table.c.logo], 
                        f"logos/{new_uuid}.png"
                    )
                    
                    upload_tasks.append((new_uuid, row_dict, seal_future, logo_future))

                # Build final insert data
                insert_data = []
                for new_uuid, row_dict, seal_future, logo_future in upload_tasks:
                    is_edi_enabled = self._to_bool(
                        self._row_value(
                            row_dict,
                            institution_table,
                            'enable_edi'
                        )
                    )

                    mapped_row = {
                        'uuid': new_uuid,
                        'name': row_dict[institution_table.c.name],
                        'description': row_dict[institution_table.c.description],
                        'type': self._map_type(row_dict[institution_table.c.institution_type]),
                        'sub_type': self._map_sub_type(
                            row_dict[institution_table.c.institution_type],
                            row_dict.get(institution_table.c.institution_sub_type)
                        ),
                        'seal': seal_future.result(),
                        'logo': logo_future.result(),
                        'address1': row_dict[address_table.c.address_line_1] if row_dict[address_table.c.id] else None,
                        'address2': row_dict[address_table.c.address_line_2] if row_dict[address_table.c.id] else None,
                        'city': row_dict[address_table.c.city] if row_dict[address_table.c.id] else None,
                        'state': self._map_state(row_dict[state_table.c.state_code]) if row_dict[address_table.c.id] else 0,
                        'zipcode': self._clean_zipcode(
                            row_dict[address_table.c.zip_code],
                            self._get_column_length(
                                dest_table,
                                "zipcode"
                            )
                        ) if row_dict[address_table.c.id] else None,
                        'country': self._map_country(row_dict[country_table.c.country_code]) if row_dict[address_table.c.id] else 0,
                        'phone_number': row_dict[institution_table.c.phone_number],
                        'legend': self._row_value(
                            row_dict,
                            institution_table,
                            'legend_file_name'
                        ),
                        'notification_email': row_dict[institution_table.c.notification_email],
                        'is_scholarship_enabled': bool(row_dict[institution_table.c.enable_scholarships]),
                        'is_transcript_enabled': bool(row_dict[institution_table.c.enable_transcript_share]),
                        'ediSegment': self._row_value(
                            row_dict,
                            institution_table,
                            'edi_segment_terminator'
                        ),
                        'ediField': self._row_value(
                            row_dict,
                            institution_table,
                            'edi_field_terminator'
                        ),
                        'marketNotificationEmail': self._row_value(
                            row_dict,
                            institution_table,
                            'mk_notification_email'
                        ),
                        'signTitle': self._row_value(
                            row_dict,
                            institution_table,
                            'signature_title'
                        ),
                        'industry': self._map_industry(
                            self._row_value(
                                row_dict,
                                institution_table,
                                'company_type'
                            )
                        ),
                        'isVirtualJobFairAdministrationEnabled': self._to_bool(
                            self._row_value(
                                row_dict,
                                institution_table,
                                'enable_virtual_job_fair'
                            )
                        ),
                        'institution_status': 2 if row_dict[institution_table.c.active]==0 else 1,
                    }
                    self._set_if_column_exists(
                        mapped_row,
                        dest_table,
                        (
                            'is_edi_enabled',
                            'isEdiEnabled'
                        ),
                        is_edi_enabled
                    )
                    if 'qual_code' in dest_table.c:
                        mapped_row['qual_code'] = self._row_value(
                            row_dict,
                            institution_table,
                            'qual_code'
                        )
                    if 'nsc_receiver_id' in dest_table.c:
                        mapped_row['nsc_receiver_id'] = self._row_value(
                            row_dict,
                            institution_table,
                            'nsc_receiver_id'
                        )
                    insert_data.append(mapped_row)

            if insert_data:
                dest_conn.execute(insert(dest_table), insert_data)
                logger.info(f"Successfully migrated {len(insert_data)} Main Institutions.")
                return len(insert_data)
            return 0

    def _map_sub_type(self, institution_type, institution_sub_type):
        # Normalize input
        if institution_type:
            institution_type = str(institution_type).strip().lower()
        else:
            institution_type = ''
        # Treat None, empty, or whitespace as unspecified (0)
        if not institution_sub_type or str(institution_sub_type).strip() == '':
            sub_type = None
        else:
            sub_type = str(institution_sub_type).strip().lower()

        if institution_type == 'university':
            mapping = {
                None: 0,
                'high school': 1,
                'community college': 2,
                'higher education': 3,
                'nsc': 4,
                'college': 5,
            }
            return mapping.get(sub_type, 0)
        elif institution_type == 'parentuniversity':
            mapping = {
                None: 0,
                'community college': 1,
                'independent school district': 2,
            }
            return mapping.get(sub_type, 0)
        elif institution_type == 'serviceprovider':
            mapping = {
                None: 0,
                'training': 1,
                'edready': 2,
            }
            return mapping.get(sub_type, 0)
        elif institution_type == 'regionalserviceprovider':
            mapping = {
                None: 0,
                'esc': 1,
            }
            return mapping.get(sub_type, 0)
        else:
            return 0

    def _migrate_campuses(self, dest_conn) -> int:
        logger.info("Migrating Institution Campuses...")
        
        institution_table = self._manual_reflect('institution', self.source_engine, self.metadata_source)
        address_table = self._manual_reflect('address', self.source_engine, self.metadata_source)
        state_table = self._manual_reflect('state', self.source_engine, self.metadata_source)
        country_table = self._manual_reflect('country', self.source_engine, self.metadata_source)
        dest_table = self._manual_reflect('institution_campuses', self.dest_engine, self.metadata_dest)
        parent_institution_column = self._parent_institution_column(
            institution_table
        )

        query = select(institution_table, address_table, state_table, country_table).select_from(
            institution_table
            .join(address_table, institution_table.c.address_id == address_table.c.id, isouter=True)
            .join(state_table, address_table.c.state == state_table.c.id, isouter=True)
            .join(country_table, address_table.c.country == country_table.c.id, isouter=True)
        )

        allowed_types = [
            'university',
            'employer',
            'parentuniversity',
            'serviceprovider',
            'regionalServiceProvider',
            'University',
            'Employer',
            'School',
            'Service Provider',
            'Regional Service Provider'
        ]
        if self.config.get('limit') and self.engine.id_map:
            query = query.where(
                parent_institution_column.in_(list(self.engine.id_map.keys())),
                institution_table.c.institution_type.in_(allowed_types)
            )
        else:
            query = query.where(
                parent_institution_column.isnot(None),
                institution_table.c.institution_type.in_(allowed_types)
            )

        with self.source_engine.connect() as source_conn:
            results = source_conn.execute(query)
            
            insert_data = []
            for row in results:
                row_dict = row._mapping
                parent_id = row_dict[parent_institution_column]
                parent_uuid = self.engine.id_map.get(parent_id)
                
                if not parent_uuid:
                    logger.warning(f"Parent institution {parent_id} not found for campus {row_dict[institution_table.c.id]}. Skipping.")
                    continue

                mapped_row = {
                    'uuid': str(uuid.uuid4()),
                    'campus_name': row_dict.get(institution_table.c.name) or row_dict.get('name'),
                    'campus_id': row_dict.get(institution_table.c.school_code) or str(row_dict.get(institution_table.c.id)) or str(row_dict.get('id')),
                    'institution_uuid': parent_uuid,
                    'description': row_dict.get(institution_table.c.description) or row_dict.get('description'),
                    'address1': row_dict.get(address_table.c.address_line_1) if row_dict.get(address_table.c.id) else None,
                    'address2': row_dict.get(address_table.c.address_line_2) if row_dict.get(address_table.c.id) else None,
                    'city': row_dict.get(address_table.c.city) if row_dict.get(address_table.c.id) else None,
                    'state': self._map_state(row_dict.get(state_table.c.state_code)) if row_dict.get(address_table.c.id) else 0,
                    'zipcode': self._clean_zipcode(
                        row_dict.get(address_table.c.zip_code),
                        self._get_column_length(
                            dest_table,
                            "zipcode"
                        )
                    ) if row_dict.get(address_table.c.id) else None,
                    'country': self._map_country(row_dict.get(country_table.c.country_code)) if row_dict.get(address_table.c.id) else 0,
                    'phone_number': row_dict.get(institution_table.c.phone_number) or row_dict.get('phone_number'),
                    'campus_status': 2 if row_dict.get(institution_table.c.active)==0 else 1,

                }
                insert_data.append(mapped_row)

            if insert_data:
                dest_conn.execute(insert(dest_table), insert_data)
                logger.info(f"Successfully migrated {len(insert_data)} Campuses.")
                return len(insert_data)
            return 0

    def _parent_institution_column(self, institution_table):
        for column_name in (
            'parent_Institution_id',
            'parent_institution_id'
        ):
            if column_name in institution_table.c:
                return institution_table.c[column_name]

        raise ValueError(
            "Source table 'institution' is missing parent_institution_id "
            "column."
        )

    def _is_main_institution_condition(
        self,
        institution_table,
        parent_institution_column
    ):
        return or_(
            parent_institution_column.is_(None),
            parent_institution_column == institution_table.c.id
        )

    def _row_value(
        self,
        row_dict,
        table,
        column_name,
        default=None
    ):
        if column_name in table.c:
            return row_dict.get(
                table.c[column_name],
                default
            )

        return row_dict.get(
            column_name,
            default
        )

    def _to_bool(self, value):
        if value is None:
            return False

        if isinstance(value, bool):
            return value

        if isinstance(value, (int, float)):
            return value != 0

        if isinstance(value, bytes):
            return any(value)

        normalized = str(value).strip().lower()

        if normalized in (
            '1',
            'true',
            't',
            'yes',
            'y',
            'on'
        ):
            return True

        if normalized in (
            '0',
            'false',
            'f',
            'no',
            'n',
            'off',
            ''
        ):
            return False

        return bool(value)

    def _set_if_column_exists(
        self,
        row,
        table,
        column_names,
        value
    ):
        for column_name in column_names:
            if column_name in table.c:
                row[column_name] = value
                return True

        return False

    def _map_type(self, old_type):
        mapping = {
            'university': 1,
            'employer': 2,
            'parentuniversity': 3,
            'serviceprovider': 4,
            'regionalServiceProvider': 5,
        }
        return mapping.get(old_type, 0)

    def _clean_zipcode(
        self,
        zipcode,
        max_length=None
    ):
        if zipcode is None:
            return None

        cleaned = "".join(
            str(zipcode).split()
        )

        if (
            max_length
            and
            len(cleaned) > max_length
        ):

            logger.warning(
                f"Truncating zipcode "
                f"{cleaned} to {max_length} chars"
            )

            return cleaned[:max_length]

        return cleaned

    def _get_column_length(
        self,
        table,
        column_name
    ):

        if column_name not in table.c:
            return None

        return getattr(
            table.c[column_name].type,
            "length",
            None
        )

    def _map_state(self, state_code):
        if not state_code: return 0
        
        # STATE_CODE_MAP mapped to State enum values (integers)
        mapping = {
            'AL': 1, 'AK': 2, 'AZ': 4, 'AR': 5, 'CA': 6, 'CO': 7, 'CT': 8, 'DE': 9,
            'FL': 12, 'GA': 13, 'HI': 15, 'ID': 16, 'IL': 17, 'IN': 18, 'IA': 19,
            'KS': 20, 'KY': 21, 'LA': 22, 'ME': 23, 'MD': 25, 'MA': 26, 'MI': 27,
            'MN': 28, 'MS': 29, 'MO': 30, 'MT': 31, 'NE': 32, 'NV': 33, 'NH': 34,
            'NJ': 35, 'NM': 36, 'NY': 37, 'NC': 38, 'ND': 39, 'OH': 41, 'OK': 42,
            'OR': 43, 'PA': 45, 'RI': 47, 'SC': 48, 'SD': 49, 'TN': 50, 'TX': 51,
            'UT': 52, 'VT': 53, 'VA': 55, 'WA': 56, 'WV': 57, 'WI': 58, 'WY': 59
        }
        return mapping.get(state_code, 0) # Default to 0 (UNSPECIFIED)

    def _map_country(self, country_code):
        if not country_code: return 0
        # Map US/USA to 1, everything else to 0 for now based on Country enum
        mapping = {"US": 1}
        return mapping.get(country_code, 0)
    def _map_industry(self, company_type):
        if not company_type or str(company_type).strip() == '':
            return 0
        mapping = {
            'accountingfinancebanking': 1,
            'administrationhrlegal': 2,
            'avadvertisingmarketingpr': 3,
            'avadvertisingmarketingpublicrelations': 3,
            'automotive': 4,
            'aviationaerospace': 5,
            'constructionarchitecture': 6,
            'consultingservices': 7,
            'educationtraining': 8,
            'engineeringmanufacturing': 9,
            'entertainment': 10,
            'fashion': 11,
            'governmentandpublicadministration': 12,
            'governmentpublicadministration': 12,
            'healthcare': 13,
            'hospitalitytraveltourism': 14,
            'insurance': 15,
            'informationtechnologycybersecurity': 16,
            'lawpublicsafetycorrectionssecurity': 17,
            'logisticssupplychain': 18,
            'logistics': 18,
            'manufacturing': 19,
            'mediacommunicationspublishing': 20,
            'oilgaspowerutilities': 21,
            'retailwholesale': 22,
            'salesbusinessdevelopment': 23,
            'scienceresearchdevelopment': 24,
            'transportationwarehousingdistributionlogistics': 25,
            'nonprofitentity': 26,
        }
        key = self._industry_key(company_type)
        return mapping.get(key, 0)

    def _industry_key(self, value):
        return ''.join(
            char
            for char in str(value).strip().lower()
            if char.isalnum()
        )

    def normalize(self, value):
        return (
            str(value or "")
            .strip()
            .lower()
            .replace(" ", "")
        )

    def _update_existing_institutions(self, dest_conn) -> int:
        logger.info("Updating existing institutions in target database...")
        
        # Manually reflect tables to avoid FK issues
        institution_table = self._manual_reflect('institution', self.source_engine, self.metadata_source)
        dest_table = self._manual_reflect('institutions', self.dest_engine, self.metadata_dest)
        parent_institution_column = self._parent_institution_column(
            institution_table
        )
        
        if not dest_table.columns:
            logger.error("Destination table 'institutions' not found or has no columns.")
            raise ValueError("Destination table 'institutions' not found.")

        # Get all existing institutions from target db
        # We need their uuid and name to match them
        with self.dest_engine.connect() as dest_conn_read:
            dest_results = dest_conn_read.execute(select(dest_table.c.uuid, dest_table.c.name))
            dest_rows = dest_results.all()
            
        if not dest_rows:
            logger.info("No institutions found in target database to update.")
            return 0
            
        # Map normalized target name -> destination row dict
        dest_map = {}
        for row in dest_rows:
            r_dict = row._mapping
            name = r_dict.get('name')
            if name:
                norm_name = self.normalize(name)
                dest_map[norm_name] = r_dict

        # Load all source institutions
        with self.source_engine.connect() as source_conn:
            source_results = source_conn.execute(
                select(institution_table).where(
                    self._is_main_institution_condition(
                        institution_table,
                        parent_institution_column
                    )
                )
            )
            source_rows = source_results.all()

        if not source_rows:
            logger.info("No institutions found in source database.")
            return 0

        update_count = 0
        upload_tasks = []
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            for s_row in source_rows:
                s_dict = s_row._mapping
                s_name = s_dict.get('name')
                if not s_name:
                    continue
                norm_s_name = self.normalize(s_name)
                if norm_s_name not in dest_map:
                    continue
                
                dest_inst = dest_map[norm_s_name]
                dest_uuid = dest_inst.get('uuid')
                
                seal_val = self._row_value(
                    s_dict,
                    institution_table,
                    'institution_seal'
                )
                logo_val = self._row_value(
                    s_dict,
                    institution_table,
                    'logo'
                )
                
                seal_future = executor.submit(
                    self.storage.upload_base64, 
                    seal_val, 
                    f"seals/{dest_uuid}.png"
                )
                logo_future = executor.submit(
                    self.storage.upload_base64, 
                    logo_val, 
                    f"logos/{dest_uuid}.png"
                )
                
                upload_tasks.append((dest_uuid, s_dict, seal_future, logo_future))
                
            for dest_uuid, s_dict, seal_future, logo_future in upload_tasks:
                seal_url = seal_future.result()
                logo_url = logo_future.result()
                
                sub_type_val = self._map_sub_type(
                    self._row_value(
                        s_dict,
                        institution_table,
                        'institution_type'
                    ),
                    self._row_value(
                        s_dict,
                        institution_table,
                        'institution_sub_type'
                    )
                )
                
                legend_val = self._row_value(
                    s_dict,
                    institution_table,
                    'legend_file_name'
                )
                
                is_transcript_enabled_val = self._to_bool(
                    self._row_value(
                        s_dict,
                        institution_table,
                        'enable_transcript_share'
                    )
                )
                
                is_edi_enabled_val = self._to_bool(
                    self._row_value(
                        s_dict,
                        institution_table,
                        'enable_edi'
                    )
                )
                
                edi_segment_val = self._row_value(
                    s_dict,
                    institution_table,
                    'edi_segment_terminator'
                )
                
                edi_field_val = self._row_value(
                    s_dict,
                    institution_table,
                    'edi_field_terminator'
                )
                
                market_notification_email_val = self._row_value(
                    s_dict,
                    institution_table,
                    'mk_notification_email'
                )
                
                sign_title_val = self._row_value(
                    s_dict,
                    institution_table,
                    'signature_title'
                )
                
                industry_val = self._map_industry(
                    self._row_value(
                        s_dict,
                        institution_table,
                        'company_type'
                    )
                )
                
                is_virtual_job_fair_val = self._to_bool(
                    self._row_value(
                        s_dict,
                        institution_table,
                        'enable_virtual_job_fair'
                    )
                )

                update_values = {
                    'sub_type': sub_type_val,
                    'legend': legend_val,
                    'is_transcript_enabled': is_transcript_enabled_val,
                    'ediSegment': edi_segment_val,
                    'ediField': edi_field_val,
                    'marketNotificationEmail': market_notification_email_val,
                    'signTitle': sign_title_val,
                    'industry': industry_val,
                    'isVirtualJobFairAdministrationEnabled': is_virtual_job_fair_val,
                    'logo': logo_url,
                    'seal': seal_url
                }

                self._set_if_column_exists(
                    update_values,
                    dest_table,
                    (
                        'is_edi_enabled',
                        'isEdiEnabled'
                    ),
                    is_edi_enabled_val
                )

                if 'qual_code' in dest_table.c:
                    update_values['qual_code'] = self._row_value(
                        s_dict,
                        institution_table,
                        'qual_code'
                    )

                if 'nsc_receiver_id' in dest_table.c:
                    update_values['nsc_receiver_id'] = self._row_value(
                        s_dict,
                        institution_table,
                        'nsc_receiver_id'
                    )
                
                update_stmt = dest_table.update().where(
                    dest_table.c.uuid == dest_uuid
                ).values(update_values)
                
                dest_conn.execute(update_stmt)
                update_count += 1
                
        logger.info(f"Successfully updated {update_count} existing institutions.")
        return update_count

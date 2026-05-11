import uuid
import logging
from sqlalchemy import select, insert
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
            
            return count_main + count_campuses

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

        # Build join query and filter by parent_id and allowed types
        allowed_types = ['University', 'Employer', 'School', 'Service Provider', 'Regional Service Provider']
        query = select(institution_table, address_table, state_table, country_table).select_from(
            institution_table
            .join(address_table, institution_table.c.address_id == address_table.c.id, isouter=True)
            .join(state_table, address_table.c.state == state_table.c.id, isouter=True)
            .join(country_table, address_table.c.country == country_table.c.id, isouter=True)
        ).where(
            institution_table.c.parent_Institution_id == None,
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
                    mapped_row = {
                        'uuid': new_uuid,
                        'name': row_dict[institution_table.c.name],
                        'description': row_dict[institution_table.c.description],
                        'type': self._map_type(row_dict[institution_table.c.institution_type]),
                        'seal': seal_future.result(),
                        'logo': logo_future.result(),
                        'address1': row_dict[address_table.c.address_line_1] if row_dict[address_table.c.id] else None,
                        'address2': row_dict[address_table.c.address_line_2] if row_dict[address_table.c.id] else None,
                        'city': row_dict[address_table.c.city] if row_dict[address_table.c.id] else None,
                        'state': self._map_state(row_dict[state_table.c.state_code]) if row_dict[address_table.c.id] else 0,
                        'zipcode': row_dict[address_table.c.zip_code] if row_dict[address_table.c.id] else None,
                        'country': self._map_country(row_dict[country_table.c.country_code]) if row_dict[address_table.c.id] else 0,
                        'phone_number': row_dict[institution_table.c.phone_number],
                        'notification_email': row_dict[institution_table.c.notification_email],
                        'is_scholarship_enabled': bool(row_dict[institution_table.c.enable_scholarships]),
                        'institution_status': 1 if row_dict[institution_table.c.active] else 2,
                    }
                    insert_data.append(mapped_row)

            if insert_data:
                dest_conn.execute(insert(dest_table), insert_data)
                logger.info(f"Successfully migrated {len(insert_data)} Main Institutions.")
                return len(insert_data)
            return 0

    def _migrate_campuses(self, dest_conn) -> int:
        logger.info("Migrating Institution Campuses...")
        
        institution_table = self._manual_reflect('institution', self.source_engine, self.metadata_source)
        address_table = self._manual_reflect('address', self.source_engine, self.metadata_source)
        state_table = self._manual_reflect('state', self.source_engine, self.metadata_source)
        country_table = self._manual_reflect('country', self.source_engine, self.metadata_source)
        dest_table = self._manual_reflect('institution_campuses', self.dest_engine, self.metadata_dest)

        query = select(institution_table, address_table, state_table, country_table).select_from(
            institution_table
            .join(address_table, institution_table.c.address_id == address_table.c.id, isouter=True)
            .join(state_table, address_table.c.state == state_table.c.id, isouter=True)
            .join(country_table, address_table.c.country == country_table.c.id, isouter=True)
        )

        allowed_types = ['University', 'Employer', 'School', 'Service Provider', 'Regional Service Provider']
        if self.config.get('limit') and self.engine.id_map:
            query = query.where(
                institution_table.c.parent_Institution_id.in_(list(self.engine.id_map.keys())),
                institution_table.c.institution_type.in_(allowed_types)
            )
        else:
            query = query.where(
                institution_table.c.parent_Institution_id != None,
                institution_table.c.institution_type.in_(allowed_types)
            )

        with self.source_engine.connect() as source_conn:
            results = source_conn.execute(query)
            
            insert_data = []
            for row in results:
                row_dict = row._mapping
                parent_id = row_dict[institution_table.c.parent_Institution_id]
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
                    'zipcode': row_dict.get(address_table.c.zip_code) if row_dict.get(address_table.c.id) else None,
                    'country': self._map_country(row_dict.get(country_table.c.country_code)) if row_dict.get(address_table.c.id) else 0,
                    'phone_number': row_dict.get(institution_table.c.phone_number) or row_dict.get('phone_number'),
                    'campus_status': 1 if row_dict.get(institution_table.c.active) or row_dict.get('active') else 2,
                }
                insert_data.append(mapped_row)

            if insert_data:
                dest_conn.execute(insert(dest_table), insert_data)
                logger.info(f"Successfully migrated {len(insert_data)} Campuses.")
                return len(insert_data)
            return 0

    def _map_type(self, old_type):
        mapping = {
            'university': 1,
            'employer': 2,
            'School': 1,
            'serviceprovider': 4,
            'regionalServiceProvider': 5,
        }
        return mapping.get(old_type, 0)

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

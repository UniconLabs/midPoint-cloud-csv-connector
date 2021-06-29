package com.evolveum.polygon.connector.cloud.objectstorage.csv;

import com.evolveum.polygon.connector.cloud.objectstorage.csv.util.Column;
import com.evolveum.polygon.connector.cloud.objectstorage.csv.util.StringAccessor;
import com.evolveum.polygon.connector.cloud.objectstorage.csv.util.Util;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.output.StringBuilderWriter;
import org.identityconnectors.common.Pair;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.*;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.common.objects.filter.FilterTranslator;
import org.identityconnectors.framework.spi.SyncTokenResultsHandler;
import org.identityconnectors.framework.spi.operations.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import static com.evolveum.polygon.connector.cloud.objectstorage.csv.util.Util.handleGenericException;


public class CloudCsvProcessor implements CreateOp, DeleteOp, TestOp, SearchOp<String>,
		UpdateAttributeValuesOp, AuthenticateOp, ResolveUsernameOp, SyncOp {

	private enum Operation {

		DELETE, UPDATE, ADD_ATTR_VALUE, REMOVE_ATTR_VALUE
	}

	private static final Log LOG = Log.getLog(CloudCsvProcessor.class);

	private final CloudCsvConfiguration configuration;

	private final Map<String, Column> header;

	private final CloudStorageService cloudStorageService;

	private final Map<String, String> syncFiles = new HashMap<>();


	public CloudCsvProcessor(final CloudCsvConfiguration configuration) throws Exception {
		this.configuration = configuration;
		this.cloudStorageService = CloudStorageServiceFactory.getCloudServiceProvider(configuration);
		this.verifyCloudProviderObjectStorageConnection();
		this.header = initHeader(null);
	}

	private void verifyCloudProviderObjectStorageConnection() {
		if (configuration.isReadOnly()) {
			try {
				cloudStorageService.checkFileExistsAndCanRead(configuration);
			} catch (Exception e) {
				handleGenericException(e, "" +
						"Error occurred while Reading File " + configuration.getFileName() +
						" in bucket " + configuration.getBucketName() + " in the Cloud Object Storage Provider " +
						configuration.getCloudObjectStorageProvider() + " . Since read only is set, test fails.");
			}
		} else {
			try {
				cloudStorageService.createBucketIfNotExists(configuration);
			} catch (Exception e) {
				handleGenericException(e, "Error checking Cloud Object Storage bucket " +
						configuration.getBucketName() + " in the Cloud Object Provider "
						+ configuration.getCloudObjectStorageProvider()
						+ " exists and creating this bucket if it doesn't exist!");
			}
		}
	}

	private Map<String, Column> initHeader(final File optionalPhysicalFile) {
		synchronized (CloudCsvObjectStorageConnector.SYNCH_FILE_LOCK) {
			try {
				Reader reader = null;

				try {
					if (optionalPhysicalFile == null) {
						reader = cloudStorageService.getFileAsReader(configuration);
					} else {
						reader = Util.createReader(optionalPhysicalFile, configuration);
					}
				} catch (Exception e) {
					//swallow for now, file whether local or in object storage must not exist
				}

				if (reader == null) {
					return createHeader(null);
				}

				final CSVFormat csv = Util.createCsvFormat(configuration);
				final CSVParser parser = csv.parse(reader);
				final Iterator<CSVRecord> iterator = parser.iterator();

				CSVRecord record = null;
				while (iterator.hasNext()) {
					record = iterator.next();
					if (!isRecordEmpty(record)) {
						break;
					}
				}

				if (record == null) {
					throw new ConfigurationException("Couldn't initialize headers, nothing in csv file for object class "
							+ configuration.getObjectClass());
				}

				return createHeader(record);

			} catch (Exception ex) {
				throw new ConnectorIOException("Couldn't initialize connector for object class "
						+ configuration.getObjectClass(), ex);
			}
		}
	}

	private String getAvailableAttributeName(Map<String, Column> header, String realName) {
		String availableName = realName;
		for (int i = 1; i <= header.size(); i++) {
			if (!header.containsKey(availableName)) {
				break;
			}

			availableName = realName + i;
		}

		return availableName;
	}

	private Map<String, Column> createHeader(CSVRecord record) {
		final Map<String, Column> header = new HashMap<>();

		if (record != null) {
			if (configuration.isHeaderExists()) {
				for (int i = 0; i < record.size(); i++) {
					String name = record.get(i);

					if (StringUtil.isEmpty(name)) {
						name = Util.DEFAULT_COLUMN_NAME + 0;
					}

					String availableName = getAvailableAttributeName(header, name);
					header.put(availableName, new Column(name, i));
				}
			} else {
				// header doesn't exist, we just create col0...colN
				for (int i = 0; i < record.size(); i++) {
					header.put(Util.DEFAULT_COLUMN_NAME + i, new Column(null, i));
				}
			}
		}

		LOG.ok("Created header {0}", header);
		testHeader(header);
		return header; //TODO if this is empty does that break things?
	}

	private void testHeader(Map<String, Column> headers) {
		if (headers != null && !headers.isEmpty()) {
			boolean uniqueFound = false;
			boolean passwordFound = false;

			for (String header : headers.keySet()) {
				if (header.equalsIgnoreCase(configuration.getUniqueAttribute())) {
					uniqueFound = true;
					continue;
				}

				if (header.equalsIgnoreCase(configuration.getPasswordAttribute())) {
					passwordFound = true;
					continue;
				}

				if (uniqueFound && passwordFound) {
					break;
				}
			}

			if (!uniqueFound) {
				throw new ConfigurationException("Header in csv file doesn't contain "
						+ "unique attribute name as defined in configuration."); //TODO does this really need to be exception?
			}

			if (StringUtil.isNotEmpty(configuration.getPasswordAttribute()) && !passwordFound) {
				throw new ConfigurationException("Header in csv file doesn't contain "
						+ "password attribute name as defined in configuration."); //TODO does this really need to be exception?
			}
		}
	}

	public ObjectClass getObjectClass() {
		return configuration.getObjectClass();
	}

	public void schema(SchemaBuilder schema) {
		try {
			final ObjectClassInfoBuilder objClassBuilder = new ObjectClassInfoBuilder();
			objClassBuilder.setType(getObjectClass().getObjectClassValue());
			objClassBuilder.setAuxiliary(configuration.isAuxiliary());
			objClassBuilder.setContainer(configuration.isContainer());
			objClassBuilder.addAllAttributeInfo(createAttributeInfo(header));

			schema.defineObjectClass(objClassBuilder.build());

		} catch (Exception ex) {
			handleGenericException(ex, "Couldn't initialize connector");
		}
	}

	private List<AttributeInfo> createAttributeInfo(Map<String, Column> columns) {
		final List<AttributeInfo> infos = new ArrayList<>();

		if (columns != null && !columns.isEmpty()) {

			List<String> multivalueAttributes = new ArrayList<>();
			if (StringUtil.isNotEmpty(configuration.getMultivalueAttributes())) {
				final String[] array = configuration.getMultivalueAttributes().split(configuration.getMultivalueDelimiter());
				multivalueAttributes = Arrays.asList(array);
			}

			for (String name : columns.keySet()) {
				if (name == null || name.isEmpty()) {
					continue;
				}

				if (name.equalsIgnoreCase(configuration.getUniqueAttribute())) {
					// unique column
					AttributeInfoBuilder builder = new AttributeInfoBuilder(Uid.NAME);
					builder.setType(String.class);
					builder.setNativeName(name);

					infos.add(builder.build());

					if (!isUniqueAndNameAttributeEqual()) {
						builder = new AttributeInfoBuilder(name);
						builder.setType(String.class);
						builder.setNativeName(name);
						builder.setRequired(true);

						infos.add(builder.build());

						continue;
					}
				}

				if (name.equalsIgnoreCase(configuration.getNameAttribute())) {
					final AttributeInfoBuilder builder = new AttributeInfoBuilder(Name.NAME);
					builder.setType(String.class);
					builder.setNativeName(name);

					if (isUniqueAndNameAttributeEqual()) {
						builder.setRequired(true);
					}

					infos.add(builder.build());

					continue;
				}

				if (name.equalsIgnoreCase(configuration.getPasswordAttribute())) {
					final AttributeInfoBuilder builder = new AttributeInfoBuilder(OperationalAttributes.PASSWORD_NAME);
					builder.setType(GuardedString.class);
					builder.setNativeName(name);

					infos.add(builder.build());

					continue;
				}

				final AttributeInfoBuilder builder = new AttributeInfoBuilder(name);
				if (name.equalsIgnoreCase(configuration.getPasswordAttribute())) {
					builder.setType(GuardedString.class);
				} else {
					builder.setType(String.class);
				}
				builder.setNativeName(name);
				if (multivalueAttributes.contains(name)) {
					builder.setMultiValued(true);
				}

				infos.add(builder.build());
			}
		}

		return infos;
	}

	@Override
	public Uid authenticate(ObjectClass oc, String username, GuardedString password, OperationOptions oo) {
		return resolveUsername(username, password, true);
	}

	@Override
	public Uid create(ObjectClass oc, Set<Attribute> set, OperationOptions oo) {
		if (configuration.isReadOnly()) {
			throw new ConnectorException("Can't add attribute values. Readonly set to true.");
		}

		final Set<Attribute> attributes = normalize(set);
		final String uidValue = findUidValue(attributes);
		final Uid uid = new Uid(uidValue);

		Reader reader = null;
		Writer writer = null;
		try {
			synchronized (CloudCsvObjectStorageConnector.SYNCH_FILE_LOCK) {
				if (cloudStorageService.checkFileExistsAndCanRead(configuration)) {
					reader = cloudStorageService.getFileAsReader(configuration);
				}

				//writer = Files.newBufferedWriter(Paths.get(Util.DEFAULT_TMP_FOLDER), StandardCharsets.UTF_8);
				writer = new StringBuilderWriter();
				CSVFormat csv = Util.createCsvFormat(configuration);

				if (reader != null) {
					CSVParser parser = csv.parse(reader);
					csv = Util.createCsvFormat(configuration);
					CSVPrinter printer = csv.print(writer);

					Iterator<CSVRecord> iterator = parser.iterator();
					// we don't want to skip header in any case, but if it's there just
					// write it to tmp file as "standard" record. We can't handle first row
					// as header in case there are more columns with the same name.
					if (configuration.isHeaderExists() && iterator.hasNext()) {
						CSVRecord record = iterator.next();
						printer.printRecord(record);
					}

					// handling real records
					while (iterator.hasNext()) {
						CSVRecord record = iterator.next();
						ConnectorObject obj = createConnectorObject(record);

						if (uid.equals(obj.getUid())) {
							throw new AlreadyExistsException("Account already exists '" + uid.getUidValue() + "'.");
						}

						printer.printRecord(record);
					}

					printer.printRecord(createNewRecord(attributes));
					reader.close();

				} else {
					final CSVPrinter printer = csv.print(writer);
					if (configuration.isHeaderExists()) {
						final Set<Attribute> csvHead = new HashSet<>();
						header.keySet().stream().forEach(key -> csvHead.add(AttributeBuilder.build(key, key)));
						printer.printRecord(createNewRecord(csvHead));
					}
					printer.printRecord(createNewRecord(attributes));
				}

				writer.close();
				cloudStorageService.uploadString(configuration, writer.toString());

			}
		} catch (Exception ex) {
			handleGenericException(ex, "Error during account '" + uid + "' create");
		} finally {
			Util.cleanupResources(writer, reader, null);
		}

		return uid;
	}

	private boolean isPassword(String column) {
		return StringUtil.isNotEmpty(configuration.getPasswordAttribute())
				&& configuration.getPasswordAttribute().equalsIgnoreCase(column);
	}

	private boolean isUid(String column) {
		return configuration.getUniqueAttribute().equalsIgnoreCase(column);
	}

	private List<Object> createNewRecord(Set<Attribute> attributes) {
		final Object[] record = new Object[header.size()];

		final Attribute nameAttr = AttributeUtil.getNameFromAttributes(attributes);
		final Object name = nameAttr != null ? AttributeUtil.getSingleValue(nameAttr) : null;

		final Attribute uniqueAttr = AttributeUtil.find(configuration.getUniqueAttribute(), attributes);
		final Object uid = uniqueAttr != null ? AttributeUtil.getSingleValue(uniqueAttr) : null;

		for (String column : header.keySet()) {
			Object value;
			if (isPassword(column)) {
				Attribute attr = AttributeUtil.find(OperationalAttributes.PASSWORD_NAME, attributes);
				if (attr == null) {
					continue;
				}

				value = Util.createRawValue(attr, configuration);
			} else if (isName(column) && name != null) {
				value = name;
			} else if (isUid(column) && uid != null) {
				value = uid;
			} else {
				Attribute attr = AttributeUtil.find(column, attributes);
				if (attr == null) {
					continue;
				}

				value = Util.createRawValue(attr, configuration);
			}

			record[header.get(column).getIndex()] = value;
		}

		return Arrays.asList(record);
	}

	private String findUidValue(Set<Attribute> attributes) {
		final Attribute uniqueAttr = AttributeUtil.find(configuration.getUniqueAttribute(), attributes);
		final Object uid = uniqueAttr != null ? AttributeUtil.getSingleValue(uniqueAttr) : null;

		if (uid == null) {
			throw new InvalidAttributeValueException("Unique attribute value not defined");
		}

		return uid.toString();
	}

	@Override
	public void delete(ObjectClass oc, Uid uid, OperationOptions oo) {
		if (configuration.isReadOnly()) {
			throw new ConnectorException("Can't add attribute values. Readonly set to true.");
		}

		update(Operation.DELETE, uid, null);
	}

	@Override
	public Uid resolveUsername(ObjectClass oc, String username, OperationOptions oo) {
		return resolveUsername(username, null, false);
	}

	@Override
	public FilterTranslator<String> createFilterTranslator(ObjectClass oc, OperationOptions oo) {
		return new CsvFilterTranslator();
	}

	private boolean skipRecord(CSVRecord record) {
		if (configuration.isHeaderExists() && record.getRecordNumber() == 1) {
			return true;
		}

		return isRecordEmpty(record);
	}

	@Override
	public void executeQuery(ObjectClass oc, String uid, ResultsHandler handler, OperationOptions oo) {
		if (cloudStorageService.checkFileExistsAndCanRead(configuration)) {
			try {
				final CSVFormat csv = Util.createCsvFormatReader(configuration);
				final Reader reader = cloudStorageService.getFileAsReader(configuration);
				final CSVParser parser = csv.parse(reader);

				for (CSVRecord record : parser) {
					if (skipRecord(record)) {
						continue;
					}

					ConnectorObject obj = createConnectorObject(record);

					if (uid == null) {
						if (!handler.handle(obj)) {
							break;
						} else {
							continue;
						}
					}

					if (!uidMatches(uid, obj.getUid().getUidValue(), configuration.isIgnoreIdentifierCase())) {
						continue;
					}

					if (!handler.handle(obj)) {
						break;
					}
				}
			} catch (Exception ex) {
				handleGenericException(ex, "Error during query execution");
			}

		} else {
			LOG.warn("Returning no results because file doesn't exist or midPoint doesn't have access!");
		}
	}

	private boolean uidMatches(String uid1, String uid2, boolean ignoreCase) {
		if (uid1 != null) {
			return uid1.equalsIgnoreCase(uid2) || ignoreCase && uid1.equalsIgnoreCase(uid2);
		}
		return false;
	}

	private void validateAuthenticationInputs(String username, GuardedString password, boolean authenticate) {
		if (StringUtil.isEmpty(username)) {
			throw new InvalidCredentialException("Username must not be empty");
		}

		if (authenticate && StringUtil.isEmpty(configuration.getPasswordAttribute())) {
			throw new ConfigurationException("Password attribute not defined in configuration");
		}

		if (authenticate && password == null) {
			throw new InvalidPasswordException("Password is not defined");
		}
	}

	private Uid resolveUsername(String username, GuardedString password, boolean authenticate) {
		validateAuthenticationInputs(username, password, authenticate);

		final CSVFormat csv = Util.createCsvFormatReader(configuration);
		try {
			final Reader reader = cloudStorageService.getFileAsReader(configuration);
			final CSVParser parser = csv.parse(reader);
			final Iterator<CSVRecord> iterator = parser.iterator();
			ConnectorObject object = null;

			while (iterator.hasNext()) {
				CSVRecord record = iterator.next();
				if (skipRecord(record)) {
					continue;
				}

				ConnectorObject obj = createConnectorObject(record);

				Name name = obj.getName();
				if (name != null && username.equalsIgnoreCase(AttributeUtil.getStringValue(name))) {
					object = obj;
					break;
				}
			}

			if (object == null) {
				String message = authenticate ? "Invalid username and/or password" : "Invalid username";
				throw new InvalidCredentialException(message);
			}

			if (authenticate) {
				authenticate(username, password, object);
			}

			Uid uid = object.getUid();
			if (uid == null) {
				throw new UnknownUidException("Unique attribute doesn't have value for account '" + username + "'");
			}

			return uid;
		} catch (Exception ex) {
			handleGenericException(ex, "Error during authentication"); //TODO handle AWS Exception
		}

		return null;
	}

	private void authenticate(String username, GuardedString password, ConnectorObject foundObject) {
		final GuardedString objPassword = AttributeUtil.getPasswordValue(foundObject.getAttributes());
		if (objPassword == null) {
			throw new InvalidPasswordException("Password not defined for username '" + username + "'");
		}

		// we don't want to authenticate against empty password
		StringAccessor acc = new StringAccessor();
		objPassword.access(acc);
		if (StringUtil.isEmpty(acc.getValue())) {
			throw new InvalidPasswordException("Password not defined for username '" + username + "'");
		}

		if (!objPassword.equals(password)) {
			throw new InvalidPasswordException("Invalid username and/or password");
		}
	}

	private void handleJustNewToken(SyncToken token, SyncResultsHandler handler) {
		if (!(handler instanceof SyncTokenResultsHandler)) {
			return;
		}

		final SyncTokenResultsHandler tokenHandler = (SyncTokenResultsHandler) handler;
		tokenHandler.handleResult(token);
	}

	@Override
	public void sync(ObjectClass oc, SyncToken token, SyncResultsHandler handler, OperationOptions oo) {
		try {
			File oldCsv = null;
			if (syncFiles.containsKey(token.getValue().toString())) {
				oldCsv = new File(syncFiles.get(token));
			}

			if (false) { //TODO do we need to get from s3 here as else above?
				oldCsv = Files.createTempFile(token.getValue().toString(), null).toFile();
				syncFiles.put(token.getValue().toString(), oldCsv.getCanonicalPath());
				//cloudStorageService.getAsAFile(configuration, oldCsv);
			}

			if (oldCsv == null) {
				LOG.error("Couldn't find old csv file to create diff, finishing synchronization.");
				return;
			}
			final Pair<String, File> newSyncFile = createNewSyncFile();
			final SyncToken newSyncToken = new SyncToken(newSyncFile.getKey());
			final File newCsv = newSyncFile.getValue();
			final Integer uidIndex = header.get(configuration.getUniqueAttribute()).getIndex();

			LOG.ok("Comparing files. Old {0} (exists: {1}, size: {2}) with new {3} (exists: {4}, size: {5})",
					oldCsv.getName(), oldCsv.exists(), oldCsv.length(), newCsv.getName(), newCsv.exists(), newCsv.length());

			final Reader reader = cloudStorageService.getFileAsReader(configuration);
			final Map<String, CSVRecord> oldData = loadOldSyncFile(oldCsv);

			Set<String> oldUsedOids = new HashSet<>();

			CSVFormat csv = Util.createCsvFormatReader(configuration);

			CSVParser parser = csv.parse(reader);
			Iterator<CSVRecord> iterator = parser.iterator();

			int changesCount = 0;

			boolean shouldContinue = true;
			while (iterator.hasNext()) {
				CSVRecord record = iterator.next();
				if (skipRecord(record)) {
					continue;
				}

				String uid = record.get(uidIndex);
				if (StringUtil.isEmpty(uid)) {
					throw new ConnectorException("Unique attribute not defined for record number "
							+ record.getRecordNumber() + " in " + newCsv.getName());
				}

				SyncDelta delta = doSyncCreateOrUpdate(record, uid, oldData, oldUsedOids, newSyncToken);
				if (delta == null) {
					continue;
				}

				changesCount++;
				shouldContinue = handler.handle(delta);
				if (!shouldContinue) {
					break;
				}
			}

			if (shouldContinue) {
				changesCount += doSyncDeleted(oldData, oldUsedOids, newSyncToken, handler);
			}

			if (changesCount == 0) {
				handleJustNewToken(new SyncToken(newSyncToken), handler);
			}
		} catch (Exception ex) {
			handleGenericException(ex, "Error during synchronization");
		} finally {
			cleanupOldSyncFiles();
		}
	}

	private Map<String, CSVRecord> loadOldSyncFile(File oldCsv) {
		Map<String, Column> header = initHeader(oldCsv);
		if (!this.header.equals(header)) {
			throw new ConnectorException("Headers of sync file '" + oldCsv + "' and current csv don't match");
		}

		Integer uidIndex = header.get(configuration.getUniqueAttribute()).getIndex();

		Map<String, CSVRecord> oldData = new HashMap<>();

		CSVFormat csv = Util.createCsvFormatReader(configuration);
		try (Reader reader = Util.createReader(oldCsv, configuration)) {
			CSVParser parser = csv.parse(reader);
			Iterator<CSVRecord> iterator = parser.iterator();
			while (iterator.hasNext()) {
				CSVRecord record = iterator.next();
				if (skipRecord(record)) {
					continue;
				}

				String uid = record.get(uidIndex);
				if (StringUtil.isEmpty(uid)) {
					throw new ConnectorException("Unique attribute not defined for record number "
							+ record.getRecordNumber() + " in " + oldCsv.getName());
				}

				if (oldData.containsKey(uid)) {
					throw new ConnectorException("Unique attribute value '" + uid + "' is not unique in "
							+ oldCsv.getName());
				}

				oldData.put(uid, record);
			}
		} catch (Exception ex) {
			handleGenericException(ex, "Error during query execution");
		}

		return oldData;
	}

	private void cleanupOldSyncFiles() {
		for (final String file : syncFiles.values()) {
			try {
				LOG.info("Deleting file {0}.", file);
				Files.deleteIfExists(Paths.get(file));
			} catch (IOException e) {
				//swallow
			}
		}
	}

	private SyncDelta doSyncCreateOrUpdate(CSVRecord newRecord, String newRecordUid, Map<String, CSVRecord> oldData,
										   Set<String> oldUsedOids, SyncToken newSyncToken) {
		SyncDelta delta;

		CSVRecord oldRecord = oldData.get(newRecordUid);
		if (oldRecord == null) {
			// newRecord is new account
			delta = buildSyncDelta(SyncDeltaType.CREATE, newSyncToken, newRecord);
		} else {
			oldUsedOids.add(newRecordUid);

			// this will be an update if records aren't equal
			List old = Util.copyOf(oldRecord.iterator());
			List _new = Util.copyOf(newRecord.iterator());
			if (old.equals(_new)) {
				// record are equal, no update
				return null;
			}

			delta = buildSyncDelta(SyncDeltaType.UPDATE, newSyncToken, newRecord);
		}

		LOG.ok("Created delta {0}", delta);

		return delta;
	}

	private int doSyncDeleted(Map<String, CSVRecord> oldData, Set<String> oldUsedOids, SyncToken newSyncToken,
							  SyncResultsHandler handler) {

		int changesCount = 0;

		for (String oldUid : oldData.keySet()) {
			if (oldUsedOids.contains(oldUid)) {
				continue;
			}

			// deleted record
			CSVRecord deleted = oldData.get(oldUid);
			SyncDelta delta = buildSyncDelta(SyncDeltaType.DELETE, newSyncToken, deleted);

			LOG.ok("Created delta {0}", delta);
			changesCount++;

			if (!handler.handle(delta)) {
				break;
			}
		}

		return changesCount;
	}

	private SyncDelta buildSyncDelta(SyncDeltaType type, SyncToken token, CSVRecord record) {
		SyncDeltaBuilder builder = new SyncDeltaBuilder();
		builder.setDeltaType(type);
		builder.setObjectClass(ObjectClass.ACCOUNT);
		builder.setToken(token);

		ConnectorObject object = createConnectorObject(record);
		builder.setObject(object);

		return builder.build();
	}

	private long getTokenValue(SyncToken token) {
		if (token == null || token.getValue() == null) {
			return -1;
		}
		String object = token.getValue().toString();
		if (!object.matches("[0-9]{13}")) {
			return -1;
		}

		return Long.parseLong(object);
	}

	//TODO Refactor
	private Pair<String, File> createNewSyncFile() {
		try {
			long timestamp = System.currentTimeMillis();
			LOG.info("Creating new sync file {0}", timestamp);
			final File syncFile = File.createTempFile(configuration.getFileName(), ".sync." + timestamp);
			LOG.ok("New sync file created, name {0}, size {1}", syncFile.getName(), syncFile.length());

			final String token = Long.toString(timestamp);
			syncFiles.put(token, syncFile.getCanonicalPath());
			return Pair.of(token, syncFile);

		} catch (Exception ex) {
			handleGenericException(ex, "Error occurred while creating new sync file ");
		}
		return null;
	}

	@Override
	public SyncToken getLatestSyncToken(ObjectClass oc) {
		LOG.info("Creating token, synchronizing from \"now\".");
		final String token = createNewSyncFile().getKey();

		return new SyncToken(token);
	}

	@Override
	public void test() {
		configuration.validate();
		verifyCloudProviderObjectStorageConnection();
	}

	@Override
	public Uid addAttributeValues(ObjectClass oc, Uid uid, Set<Attribute> set, OperationOptions oo) {
		if (configuration.isReadOnly()) {
			throw new ConnectorException("Can't add attribute values. Readonly set to true.");
		}

		return update(Operation.ADD_ATTR_VALUE, uid, set);
	}

	@Override
	public Uid removeAttributeValues(ObjectClass oc, Uid uid, Set<Attribute> set, OperationOptions oo) {
		if (configuration.isReadOnly()) {
			throw new ConnectorException("Can't add attribute values. Readonly set to true.");
		}

		return update(Operation.REMOVE_ATTR_VALUE, uid, set);
	}

	@Override
	public Uid update(ObjectClass oc, Uid uid, Set<Attribute> set, OperationOptions oo) {
		if (configuration.isReadOnly()) {
			throw new ConnectorException("Can't add attribute values. Readonly set to true.");
		}

		return update(Operation.UPDATE, uid, set);
	}

	private boolean isRecordEmpty(CSVRecord record) {
		if (!configuration.isIgnoreEmptyLines()) {
			return false;
		}

		for (int i = 0; i < record.size(); i++) {
			String value = record.get(i);
			if (StringUtil.isNotBlank(value)) {
				return false;
			}
		}

		return true;
	}

	private Map<Integer, String> reverseHeaderMap() {
		Map<Integer, String> reversed = new HashMap<>();
		this.header.forEach((key, value) -> {

			reversed.put(value.getIndex(), key);
		});

		return reversed;
	}

	private int getLastIndexOfHeader() {
		return this.header.values().stream().mapToInt(Column::getIndex).filter(column -> column >= 0).max().orElse(0);
	}

	private ConnectorObject createConnectorObject(CSVRecord record) {
		ConnectorObjectBuilder builder = new ConnectorObjectBuilder();

		Map<Integer, String> header = reverseHeaderMap();

		if (configuration.isReadOnly() && header.size() != record.size()) {
			throw new ConnectorException("Number of columns in header (" + header.size()
					+ ") doesn't match number of columns for record (" + record.size()
					+ "). File row number: " + record.getRecordNumber());
		}

		for (int i = 0; i < record.size(); i++) {
			String name = header.get(i);
			String value = record.get(i);

			if (StringUtil.isEmpty(value)) {
				continue;
			}

			if (name.equalsIgnoreCase(configuration.getUniqueAttribute())) {
				builder.setUid(value);

				if (!isUniqueAndNameAttributeEqual()) {
					continue;
				}
			}

			if (name.equalsIgnoreCase(configuration.getNameAttribute())) {
				builder.setName(new Name(value));
				continue;
			}

			if (name.equalsIgnoreCase(configuration.getPasswordAttribute())) {
				builder.addAttribute(OperationalAttributes.PASSWORD_NAME, new GuardedString(value.toCharArray()));
				continue;
			}

			builder.addAttribute(name, createAttributeValues(value));
		}

		return builder.build();
	}

	private boolean isUniqueAndNameAttributeEqual() {
		String uniqueAttribute = configuration.getUniqueAttribute();
		String nameAttribute = configuration.getNameAttribute();

		return Objects.equals(uniqueAttribute, nameAttribute);
	}

	private List<String> createAttributeValues(String attributeValue) {
		List<String> values = new ArrayList<>();

		if (StringUtil.isEmpty(configuration.getMultivalueDelimiter())) {
			values.add(attributeValue);
		} else {
			String[] array = attributeValue.split(configuration.getMultivalueDelimiter());
			for (String item : array) {
				if (StringUtil.isEmpty(item)) {
					continue;
				}

				values.add(item);
			}
		}

		return values;
	}

	private Uid update(Operation operation, Uid uid, Set<Attribute> attributes) {

		Util.notNull(uid, "Uid must not be null");

		if ((Operation.ADD_ATTR_VALUE.equals(operation) || Operation.REMOVE_ATTR_VALUE.equals(operation))
				&& attributes.isEmpty()) {
			return uid;
		}

		final Map<Integer, String> header = reverseHeaderMap();
		attributes = normalize(attributes);

		Reader reader = null;
		Writer writer = null;
		try {
			synchronized (CloudCsvObjectStorageConnector.SYNCH_FILE_LOCK) {
				reader = cloudStorageService.getFileAsReader(configuration);
				writer = new StringBuilderWriter();

				boolean found = false;

				CSVFormat csv = Util.createCsvFormat(configuration);
				CSVParser parser = csv.parse(reader);

				csv = Util.createCsvFormat(configuration);
				CSVPrinter printer = csv.print(writer);

				for (CSVRecord record : parser) {
					Map<String, String> data = new HashMap<>();
					for (int i = 0; i < record.size(); i++) {
						data.put(header.get(i), record.get(i));
					}

					String recordUidValue = data.get(configuration.getUniqueAttribute());
					if (StringUtil.isEmpty(recordUidValue)) {
						continue;
					}

					if (!uidMatches(uid.getUidValue(), recordUidValue, configuration.isIgnoreIdentifierCase())) {
						printer.printRecord(record);
						continue;
					}

					found = true;

					if (!Operation.DELETE.equals(operation)) {
						List<Object> updated = updateObject(operation, data, attributes);

						int uidIndex = this.header.get(configuration.getUniqueAttribute()).getIndex();
						Object newUidValue = updated.get(uidIndex);
						uid = new Uid(newUidValue.toString());

						printer.printRecord(updated);
					}
				}

				writer.close();
				reader.close();

				if (!found) {
					throw new UnknownUidException("Account '" + uid + "' not found");
				}

				cloudStorageService.uploadString(configuration, writer.toString());
			}
		} catch (Exception ex) {
			handleGenericException(ex, "Error during account '" + uid + "' " + operation.name());
		} finally {
			Util.cleanupResources(writer, reader, null);
		}
		return uid;
	}

	private Set<Attribute> normalize(final Set<Attribute> attributes) {
		if (attributes == null) {
			return null;
		}

		final Set<Attribute> result = new HashSet<>(attributes);

		Attribute nameAttr = AttributeUtil.getNameFromAttributes(result);
		Object name = nameAttr != null ? AttributeUtil.getSingleValue(nameAttr) : null;

		Attribute uniqueAttr = AttributeUtil.find(configuration.getUniqueAttribute(), result);
		Object uid = uniqueAttr != null ? AttributeUtil.getSingleValue(uniqueAttr) : null;

		if (isUniqueAndNameAttributeEqual()) {
			if (name == null && uid != null) {
				if (nameAttr == null) {
					nameAttr = AttributeBuilder.build(Name.NAME, uid);
					result.add(nameAttr);
				}
			} else if (uid == null && name != null) {
				if (uniqueAttr == null) {
					uniqueAttr = AttributeBuilder.build(configuration.getUniqueAttribute(), name);
					result.add(uniqueAttr);
				}
			} else if (uid != null && name != null) {
				if (!name.equals(uid)) {
					throw new InvalidAttributeValueException("Unique attribute value doesn't match name attribute value");
				}
			}
		}

		int index = (getLastIndexOfHeader() > 0) ? getLastIndexOfHeader() + 1 : getLastIndexOfHeader();
		final Set<String> columns = header.keySet();
		for (Attribute attribute : result) {
			String attrName = attribute.getName();
			if (Uid.NAME.equalsIgnoreCase(attrName) || Name.NAME.equalsIgnoreCase(attrName)
					|| OperationalAttributes.PASSWORD_NAME.equalsIgnoreCase(attrName)) {
				continue;
			}

			if (!columns.contains(attrName)) {
				if (configuration.isReadOnly()) {
					throw new ConnectorException("Unknown attribute " + attrName);
				} else {
					header.put(attrName, new Column(attrName, index)); //attempt to add new field
					index++;
				}
			}

			if (!isUniqueAndNameAttributeEqual() && isName(attrName)) {
				throw new ConnectorException("Column used as " + Name.NAME + " attribute");
			}
		}

		return result;
	}

	private boolean isName(String column) {
		return configuration.getNameAttribute().equalsIgnoreCase(column);
	}

	private List<Object> updateObject(Operation operation, Map<String, String> data, Set<Attribute> attributes) {
		Object[] result = new Object[header.size()];

		// prefill actual data
		for (String column : header.keySet()) {
			result[header.get(column).getIndex()] = data.get(column);
		}

		// update data based on attributes parameter
		switch (operation) {
			case UPDATE:
				for (Attribute attribute : attributes) {
					Integer index;

					String name = attribute.getName();
					if (name.equalsIgnoreCase(Uid.NAME)) {
						index = header.get(configuration.getUniqueAttribute()).getIndex();
					} else if (name.equalsIgnoreCase(Name.NAME)) {
						index = header.get(configuration.getNameAttribute()).getIndex();
					} else if (name.equalsIgnoreCase(OperationalAttributes.PASSWORD_NAME)) {
						index = header.get(configuration.getPasswordAttribute()).getIndex();
					} else {
						index = header.get(name).getIndex();
					}

					String value = Util.createRawValue(attribute, configuration);
					result[index] = value;
				}
				break;
			case ADD_ATTR_VALUE:
			case REMOVE_ATTR_VALUE:
				for (Attribute attribute : attributes) {
					Class type = String.class;
					Integer index;

					String name = attribute.getName();
					if (name.equalsIgnoreCase(Uid.NAME)) {
						index = header.get(configuration.getUniqueAttribute()).getIndex();
					} else if (name.equalsIgnoreCase(Name.NAME)) {
						index = header.get(configuration.getNameAttribute()).getIndex();
					} else if (name.equalsIgnoreCase(OperationalAttributes.PASSWORD_NAME)) {
						index = header.get(configuration.getPasswordAttribute()).getIndex();
						type = GuardedString.class;
					} else {
						index = header.get(name).getIndex();
					}

					List<Object> current = Util.createAttributeValues((String) result[index], type, configuration);
					List<Object> updated = Operation.ADD_ATTR_VALUE.equals(operation) ?
							Util.addValues(current, attribute.getValue()) :
							Util.removeValues(current, attribute.getValue());

					if (isUid(name) && updated.size() != 1) {
						throw new IllegalArgumentException("Unique attribute '" + name + "' must contain single value");
					}

					String value = Util.createRawValue(updated, configuration);
					result[index] = value;
				}
		}

		return Arrays.asList(result);
	}
}

package com.evolveum.polygon.connector.cloud.objectstorage.csv;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.evolveum.polygon.connector.cloud.objectstorage.csv.util.Column;
import com.evolveum.polygon.connector.cloud.objectstorage.csv.util.S3Utils;
import com.evolveum.polygon.connector.cloud.objectstorage.csv.util.StringAccessor;
import com.evolveum.polygon.connector.cloud.objectstorage.csv.util.Util;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.*;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.common.objects.filter.FilterTranslator;
import org.identityconnectors.framework.spi.SyncTokenResultsHandler;
import org.identityconnectors.framework.spi.operations.*;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;

import static com.evolveum.polygon.connector.cloud.objectstorage.csv.util.Util.createSyncFileName;
import static com.evolveum.polygon.connector.cloud.objectstorage.csv.util.Util.handleGenericException;

/**
 * todo check new FileSystem().newWatchService() to create exclusive tmp file https://docs.oracle.com/javase/tutorial/essential/io/notification.html
 * <p>
 * Created by lazyman on 27/01/2017.
 */
public class ObjectClassHandler implements CreateOp, DeleteOp, TestOp, SearchOp<String>,
		UpdateAttributeValuesOp, AuthenticateOp, ResolveUsernameOp, SyncOp {

	private enum Operation {

		DELETE, UPDATE, ADD_ATTR_VALUE, REMOVE_ATTR_VALUE;
	}

	private static final Log LOG = Log.getLog(ObjectClassHandler.class);

	private ObjectClassHandlerConfiguration configuration;

	private Map<String, Column> header;

	private CloudStorageService cloudStorageService;

	public ObjectClassHandler(ObjectClassHandlerConfiguration configuration) {
		this.configuration = configuration;
		this.cloudStorageService = configuration.getCloudStorageService();
		header = initHeaderS3(configuration.getBucketName(), configuration.getFileName());
	}


	private Map<String, Column> initHeader(File csvFile) {
		synchronized (CsvCloudObjectStorageConnector.SYNCH_FILE_LOCK) {
			CSVFormat csv = Util.createCsvFormat(configuration);
			try (Reader reader = Util.createReader(csvFile, configuration)) {
				CSVParser parser = csv.parse(reader);
				Iterator<CSVRecord> iterator = parser.iterator();

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
			} catch (IOException ex) {
				throw new ConnectorIOException("Couldn't initialize connector for object class "
						+ configuration.getObjectClass(), ex);
			}
		}
	}

	private Map<String, Column> initHeaderS3(String bucketName, String fileName) {
		//TODO check the synchronized behavior
		synchronized (CsvCloudObjectStorageConnector.SYNCH_FILE_LOCK) {
			CSVFormat csv = Util.createCsvFormat(configuration);
			try (Reader reader = cloudStorageService.createReaderS3(fileName, configuration)) {
				CSVParser parser = csv.parse(reader);
				Iterator<CSVRecord> iterator = parser.iterator();

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
			} catch (IOException ex) {
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
		Map<String, Column> header = new HashMap<>();

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

		LOG.ok("Created header {0}", header);

		testHeader(header);

		return header;
	}

	private void testHeader(Map<String, Column> headers) {
		boolean uniqueFound = false;
		boolean passwordFound = false;

		for (String header : headers.keySet()) {
			if (header.equals(configuration.getUniqueAttribute())) {
				uniqueFound = true;
				continue;
			}

			if (header.equals(configuration.getPasswordAttribute())) {
				passwordFound = true;
				continue;
			}

			if (uniqueFound && passwordFound) {
				break;
			}
		}

		if (!uniqueFound) {
			throw new ConfigurationException("Header in csv file doesn't contain "
					+ "unique attribute name as defined in configuration.");
		}

		if (StringUtil.isNotEmpty(configuration.getPasswordAttribute()) && !passwordFound) {
			throw new ConfigurationException("Header in csv file doesn't contain "
					+ "password attribute name as defined in configuration.");
		}
	}

	public ObjectClass getObjectClass() {
		return configuration.getObjectClass();
	}

	public void schema(SchemaBuilder schema) {
		try {

			ObjectClassInfoBuilder objClassBuilder = new ObjectClassInfoBuilder();
			objClassBuilder.setType(getObjectClass().getObjectClassValue());
			objClassBuilder.setAuxiliary(configuration.isAuxiliary());
			objClassBuilder.setContainer(configuration.isContainer());
			objClassBuilder.addAllAttributeInfo(createAttributeInfo(header));

			schema.defineObjectClass(objClassBuilder.build());
		} catch (Exception ex) {
			handleGenericException(ex, "Couldn't initialize connector");
		}
	}

	private Set<AttributeInfo.Flags> createFlags(AttributeInfo.Flags... flags) {
		Set<AttributeInfo.Flags> set = new HashSet<>();
		set.addAll(Arrays.asList(flags));

		return set;
	}

	private List<AttributeInfo> createAttributeInfo(Map<String, Column> columns) {
		List<String> multivalueAttributes = new ArrayList<>();
		if (StringUtil.isNotEmpty(configuration.getMultivalueAttributes())) {
			String[] array = configuration.getMultivalueAttributes().split(configuration.getMultivalueDelimiter());
			multivalueAttributes = Arrays.asList(array);
		}

		List<AttributeInfo> infos = new ArrayList<>();
		for (String name : columns.keySet()) {
			if (name == null || name.isEmpty()) {
				continue;
			}

			if (name.equals(configuration.getUniqueAttribute())) {
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

			if (name.equals(configuration.getNameAttribute())) {
				AttributeInfoBuilder builder = new AttributeInfoBuilder(Name.NAME);
				builder.setType(String.class);
				builder.setNativeName(name);

				if (isUniqueAndNameAttributeEqual()) {
					builder.setRequired(true);
				}

				infos.add(builder.build());

				continue;
			}

			if (name.equals(configuration.getPasswordAttribute())) {
				AttributeInfoBuilder builder = new AttributeInfoBuilder(OperationalAttributes.PASSWORD_NAME);
				builder.setType(GuardedString.class);
				builder.setNativeName(name);

				infos.add(builder.build());

				continue;
			}

			AttributeInfoBuilder builder = new AttributeInfoBuilder(name);
			if (name.equals(configuration.getPasswordAttribute())) {
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

		return infos;
	}

	@Override
	public Uid authenticate(ObjectClass oc, String username, GuardedString password, OperationOptions oo) {
		return resolveUsername(username, password, oo, true);
	}

	@Override
	public Uid create(ObjectClass oc, Set<Attribute> set, OperationOptions oo) {
		if (configuration.isReadOnly()) {
			throw new ConnectorException("Can't add attribute values. Readonly set to true.");
		}

		Set<Attribute> attributes = normalize(set);

		String uidValue = findUidValue(attributes);
		Uid uid = new Uid(uidValue);

		FileLock lock = Util.obtainTmpFileLock(configuration);
		Reader reader = null;
		Writer writer = null;
		try {
			synchronized (CsvCloudObjectStorageConnector.SYNCH_FILE_LOCK) {
				reader = cloudStorageService.createReaderS3(configuration);
				writer = new BufferedWriter(Channels.newWriter(lock.channel(), configuration.getEncoding()));

				CSVFormat csv = Util.createCsvFormat(configuration);
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

				writer.close();
				reader.close();

				moveTmpToOrigS3();
			}
		} catch (Exception ex) {
			handleGenericException(ex, "Error during account '" + uid + "' create");
		} finally {
			Util.cleanupResources(writer, reader, lock, configuration);
		}

		return uid;
	}

	private void moveTmpToOrigS3() throws IOException {
		// moving existing file
		File tmp = Util.createTmpPath(configuration);
		cloudStorageService.uploadFileToS3(configuration.getBucketName(), configuration.getFileName(), tmp);
	}

	private boolean isPassword(String column) {
		return StringUtil.isNotEmpty(configuration.getPasswordAttribute())
				&& configuration.getPasswordAttribute().equals(column);
	}

	private boolean isUid(String column) {
		return configuration.getUniqueAttribute().equals(column);
	}

	private List<Object> createNewRecord(Set<Attribute> attributes) {
		final Object[] record = new Object[header.size()];

		Attribute nameAttr = AttributeUtil.getNameFromAttributes(attributes);
		Object name = nameAttr != null ? AttributeUtil.getSingleValue(nameAttr) : null;

		Attribute uniqueAttr = AttributeUtil.find(configuration.getUniqueAttribute(), attributes);
		Object uid = uniqueAttr != null ? AttributeUtil.getSingleValue(uniqueAttr) : null;

		for (String column : header.keySet()) {
			Object value;
			if (isPassword(column)) {
				Attribute attr = AttributeUtil.find(OperationalAttributes.PASSWORD_NAME, attributes);
				if (attr == null) {
					continue;
				}

				value = Util.createRawValue(attr, configuration);
			} else if (isName(column)) {
				value = name;
			} else if (isUid(column)) {
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
		Attribute uniqueAttr = AttributeUtil.find(configuration.getUniqueAttribute(), attributes);
		Object uid = uniqueAttr != null ? AttributeUtil.getSingleValue(uniqueAttr) : null;

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

		update(Operation.DELETE, oc, uid, null, oo);
	}

	@Override
	public Uid resolveUsername(ObjectClass oc, String username, OperationOptions oo) {
		return resolveUsername(username, null, oo, false);
	}

	@Override
	public FilterTranslator<String> createFilterTranslator(ObjectClass oc, OperationOptions oo) {
		return new CsvFilterTranslator();
	}

	private boolean skipRecord(CSVRecord record) {
		if (configuration.isHeaderExists() && record.getRecordNumber() == 1) {
			return true;
		}

		if (isRecordEmpty(record)) {
			return true;
		}

		return false;
	}

	@Override
	public void executeQuery(ObjectClass oc, String uid, ResultsHandler handler, OperationOptions oo) {
		CSVFormat csv = Util.createCsvFormatReader(configuration);
		try (Reader reader = cloudStorageService.createReaderS3(configuration)) {

			CSVParser parser = csv.parse(reader);
			Iterator<CSVRecord> iterator = parser.iterator();
			while (iterator.hasNext()) {
				CSVRecord record = iterator.next();
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
	}

	private boolean uidMatches(String uid1, String uid2, boolean ignoreCase) {
		return uid1.equals(uid2) || ignoreCase && uid1.equalsIgnoreCase(uid2);
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

	private Uid resolveUsername(String username, GuardedString password, OperationOptions oo, boolean authenticate) {
		validateAuthenticationInputs(username, password, authenticate);

		CSVFormat csv = Util.createCsvFormatReader(configuration);
		try (Reader reader = cloudStorageService.createReaderS3(configuration)) {

			ConnectorObject object = null;

			CSVParser parser = csv.parse(reader);
			Iterator<CSVRecord> iterator = parser.iterator();
			while (iterator.hasNext()) {
				CSVRecord record = iterator.next();
				if (skipRecord(record)) {
					continue;
				}

				ConnectorObject obj = createConnectorObject(record);

				Name name = obj.getName();
				if (name != null && username.equals(AttributeUtil.getStringValue(name))) {
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
			handleGenericException(ex, "Error during authentication");
		}

		return null;
	}

	private void authenticate(String username, GuardedString password, ConnectorObject foundObject) {
		GuardedString objPassword = AttributeUtil.getPasswordValue(foundObject.getAttributes());
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

		SyncTokenResultsHandler tokenHandler = (SyncTokenResultsHandler) handler;
		tokenHandler.handleResult(token);
	}

	@Override
	public void sync(ObjectClass oc, SyncToken token, SyncResultsHandler handler, OperationOptions oo) {
		File syncLockFile = Util.createSyncLockFile(configuration);
		FileLock lock = Util.obtainTmpFileLock(syncLockFile);

		try {
			long tokenLongValue = getTokenValue(token);
			LOG.info("Token {0}", tokenLongValue);

			if (tokenLongValue == -1) {
				//token doesn't exist, we only create new sync file - we're synchronizing from now on
				String newToken = createNewSyncFile();
				handleJustNewToken(new SyncToken(newToken), handler);
				LOG.info("Token value was not defined {0}, only creating new sync file, synchronizing from now on.", token);
				return;
			}

			doSync(tokenLongValue, handler);
		} finally {
			Util.closeQuietly(lock);
			syncLockFile.delete();
		}
	}

	private File findOldCsv(long token, String newToken, SyncResultsHandler handler) {
		File oldCsv = Util.createSyncFileName(token, configuration);
		if (!oldCsv.exists()) {
			// we'll try to find first sync file which is newer than token (there's a possibility
			// that we loose some changes this way - same as for example ldap)
			oldCsv = Util.findOldestSyncFile(token, configuration);
			if (oldCsv == null || oldCsv.equals(createSyncFileName(Long.parseLong(newToken), configuration))) {
				// we didn't found any newer file, we should stop and handle this situation as if this
				// is first time we're doing sync operation (like getLatestSyncToken())
				handleJustNewToken(new SyncToken(newToken), handler);
				LOG.info("File for token wasn't found, sync will stop, new token {0} will be returned.", token);
				return null;
			}
		}

		return oldCsv;
	}

	//TODO DIEGO: S3
	private void doSync(long token, SyncResultsHandler handler) {
		String newToken = createNewSyncFile();
		SyncToken newSyncToken = new SyncToken(newToken);

		File newCsv = Util.createSyncFileName(Long.parseLong(newToken), configuration);

		Integer uidIndex = header.get(configuration.getUniqueAttribute()).getIndex();

		File oldCsv = findOldCsv(token, newToken, handler);
		if (oldCsv == null) {
			LOG.error("Couldn't find old csv file to create diff, finishing synchronization.");
			return;
		}

		LOG.ok("Comparing files. Old {0} (exists: {1}, size: {2}) with new {3} (exists: {4}, size: {5})",
				oldCsv.getName(), oldCsv.exists(), oldCsv.length(), newCsv.getName(), newCsv.exists(), newCsv.length());

		try (Reader reader = Util.createReader(newCsv, configuration)) {
			Map<String, CSVRecord> oldData = loadOldSyncFile(oldCsv);

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

				SyncDelta delta = doSyncCreateOrUpdate(record, uid, oldData, oldUsedOids, newSyncToken, handler);
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
				handleJustNewToken(new SyncToken(newToken), handler);
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
		String[] tokenFiles = Util.listTokenFiles(configuration);
		Arrays.sort(tokenFiles);

		int preserve = configuration.getPreserveOldSyncFiles();
		if (preserve <= 1) {
			LOG.info("Not removing old token files. Preserve last tokens: {0}.", preserve);
			return;
		}

		File parentFolder = configuration.getTmpFolder();
		for (int i = 0; i + preserve < tokenFiles.length; i++) {
			File tokenSyncFile = new File(parentFolder, tokenFiles[i]);
			if (!tokenSyncFile.exists()) {
				continue;
			}

			LOG.info("Deleting file {0}.", tokenSyncFile.getName());
			tokenSyncFile.delete();
		}
	}

	private SyncDelta doSyncCreateOrUpdate(CSVRecord newRecord, String newRecordUid, Map<String, CSVRecord> oldData,
										   Set<String> oldUsedOids, SyncToken newSyncToken, SyncResultsHandler handler) {
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

	private String createNewSyncFile() {
		long timestamp = System.currentTimeMillis();

		String token = null;
		try {
			File last = Util.createSyncFileName(timestamp, configuration);
			LOG.info("Creating new sync file {0} file {1}", timestamp, last.getName());
			cloudStorageService.getInAFile(configuration.getBucketName(),configuration.getFileName(), last);
			LOG.ok("New sync file created, name {0}, size {1}", last.getName(), last.length());

			token = Long.toString(timestamp);
		} catch (IOException ex) {
			handleGenericException(ex, "Error occurred while creating new sync file " + timestamp);
		}

		return token;
	}

	@Override
	public SyncToken getLatestSyncToken(ObjectClass oc) {
		String token;
		LOG.info("Creating token, synchronizing from \"now\".");
		token = createNewSyncFile();

		return new SyncToken(token);
	}

	@Override
	public void test() {
		configuration.validate();

		initHeaderS3(configuration.getBucketName(),configuration.getFileName());
	}

	@Override
	public Uid addAttributeValues(ObjectClass oc, Uid uid, Set<Attribute> set, OperationOptions oo) {
		if (configuration.isReadOnly()) {
			throw new ConnectorException("Can't add attribute values. Readonly set to true.");
		}

		return update(Operation.ADD_ATTR_VALUE, oc, uid, set, oo);
	}

	@Override
	public Uid removeAttributeValues(ObjectClass oc, Uid uid, Set<Attribute> set, OperationOptions oo) {
		if (configuration.isReadOnly()) {
			throw new ConnectorException("Can't add attribute values. Readonly set to true.");
		}

		return update(Operation.REMOVE_ATTR_VALUE, oc, uid, set, oo);
	}

	@Override
	public Uid update(ObjectClass oc, Uid uid, Set<Attribute> set, OperationOptions oo) {
		if (configuration.isReadOnly()) {
			throw new ConnectorException("Can't add attribute values. Readonly set to true.");
		}

		return update(Operation.UPDATE, oc, uid, set, oo);
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

	private ConnectorObject createConnectorObject(CSVRecord record) {
		ConnectorObjectBuilder builder = new ConnectorObjectBuilder();

		Map<Integer, String> header = reverseHeaderMap();

		if (header.size() != record.size()) {
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

			if (name.equals(configuration.getUniqueAttribute())) {
				builder.setUid(value);

				if (!isUniqueAndNameAttributeEqual()) {
					continue;
				}
			}

			if (name.equals(configuration.getNameAttribute())) {
				builder.setName(new Name(value));
				continue;
			}

			if (name.equals(configuration.getPasswordAttribute())) {
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

		return uniqueAttribute == null ? nameAttribute == null : uniqueAttribute.equals(nameAttribute);
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

	private Uid update(Operation operation, ObjectClass objectClass, Uid uid, Set<Attribute> attributes,
					   OperationOptions oo) {

		Util.notNull(uid, "Uid must not be null");

		if ((Operation.ADD_ATTR_VALUE.equals(operation) || Operation.REMOVE_ATTR_VALUE.equals(operation))
				&& attributes.isEmpty()) {
			return uid;
		}

		Map<Integer, String> header = reverseHeaderMap();

		attributes = normalize(attributes);

		FileLock lock = Util.obtainTmpFileLock(configuration);
		Reader reader = null;
		Writer writer = null;
		try {
			synchronized (CsvCloudObjectStorageConnector.SYNCH_FILE_LOCK) {
				reader = cloudStorageService.createReaderS3(configuration);
				writer = new BufferedWriter(Channels.newWriter(lock.channel(), configuration.getEncoding()));

				boolean found = false;

				CSVFormat csv = Util.createCsvFormat(configuration);
				CSVParser parser = csv.parse(reader);

				csv = Util.createCsvFormat(configuration);
				CSVPrinter printer = csv.print(writer);

				Iterator<CSVRecord> iterator = parser.iterator();
				while (iterator.hasNext()) {
					CSVRecord record = iterator.next();

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

				moveTmpToOrigS3();
			}
		} catch (Exception ex) {
			handleGenericException(ex, "Error during account '" + uid + "' " + operation.name());
		} finally {
			Util.cleanupResources(writer, reader, lock, configuration);
		}
		return uid;
	}

	private Set<Attribute> normalize(Set<Attribute> attributes) {
		if (attributes == null) {
			return null;
		}

		Set<Attribute> result = new HashSet<>();
		result.addAll(attributes);

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

		Set<String> columns = header.keySet();
		for (Attribute attribute : result) {
			String attrName = attribute.getName();
			if (Uid.NAME.equals(attrName) || Name.NAME.equals(attrName)
					|| OperationalAttributes.PASSWORD_NAME.equals(attrName)) {
				continue;
			}

			if (!columns.contains(attrName)) {
				throw new ConnectorException("Unknown attribute " + attrName);
			}

			if (!isUniqueAndNameAttributeEqual() && isName(attrName)) {
				throw new ConnectorException("Column used as " + Name.NAME + " attribute");
			}
		}

		return result;
	}

	private boolean isName(String column) {
		return configuration.getNameAttribute().equals(column);
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
					if (name.equals(Uid.NAME)) {
						index = header.get(configuration.getUniqueAttribute()).getIndex();
					} else if (name.equals(Name.NAME)) {
						index = header.get(configuration.getNameAttribute()).getIndex();
					} else if (name.equals(OperationalAttributes.PASSWORD_NAME)) {
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
					if (name.equals(Uid.NAME)) {
						index = header.get(configuration.getUniqueAttribute()).getIndex();
					} else if (name.equals(Name.NAME)) {
						index = header.get(configuration.getNameAttribute()).getIndex();
					} else if (name.equals(OperationalAttributes.PASSWORD_NAME)) {
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

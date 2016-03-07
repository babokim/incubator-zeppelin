/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.rest;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;

import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.conf.ZeppelinConfiguration.ConfVars;
import org.apache.zeppelin.interpreter.InterpreterSetting;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Notebook;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.rest.message.CronRequest;
import org.apache.zeppelin.rest.message.InterpreterSettingListForNoteBind;
import org.apache.zeppelin.rest.message.NewNotebookRequest;
import org.apache.zeppelin.rest.message.NewParagraphRequest;
import org.apache.zeppelin.rest.message.RunParagraphWithParametersRequest;
import org.apache.zeppelin.search.SearchService;
import org.apache.zeppelin.server.JsonResponse;
import org.apache.zeppelin.socket.NotebookServer;
import org.apache.zeppelin.utils.SecurityUtils;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;

/**
 * Rest api endpoint for the noteBook.
 */
@Path("/notebook")
@Produces("application/json")
public class NotebookRestApi {
  private static final Logger LOG = LoggerFactory.getLogger(NotebookRestApi.class);
  Gson gson = new Gson();
  private Notebook notebook;
  private NotebookServer notebookServer;
  private SearchService notebookIndex;
  private ZeppelinConfiguration conf;

  public NotebookRestApi() {}

  public NotebookRestApi(Notebook notebook, NotebookServer notebookServer, SearchService search) {
    this.notebook = notebook;
    this.notebookServer = notebookServer;
    this.notebookIndex = search;
    this.conf = ZeppelinConfiguration.create();

  }

  /**
   * list note owners
   */
  @GET
  @Path("{noteId}/permissions")
  public Response getNotePermissions(@PathParam("noteId") String noteId) {
    HashSet<String> roles = SecurityUtils.getRoles();
    if (!(roles.contains("admin") || roles.contains("dev"))) {
      return new JsonResponse<>(Status.FORBIDDEN, "No permission.").build();
    }
    Note note = notebook.getNote(noteId);
    HashMap<String, HashSet> permissionsMap = new HashMap<String, HashSet>();
    permissionsMap.put("owners", note.getOwners());
    permissionsMap.put("readers", note.getReaders());
    permissionsMap.put("writers", note.getWriters());
    return new JsonResponse<>(Status.OK, "", permissionsMap).build();
  }

  String ownerPermissionError(HashSet<String> current,
                              HashSet<String> allowed) throws IOException {
    LOG.info("Cannot change permissions. Connection owners {}. Allowed owners {}",
        current.toString(), allowed.toString());
    return "Insufficient privileges to change permissions.\n\n" +
            "Allowed owners: " + allowed.toString() + "\n\n" +
            "User belongs to: " + current.toString();
  }

  /**
   * Set note owners
   */
  @PUT
  @Path("{noteId}/permissions")
  public Response putNotePermissions(@PathParam("noteId") String noteId, String req)
      throws IOException {
    HashSet<String> roles = SecurityUtils.getRoles();
    if (!(roles.contains("admin") || roles.contains("dev"))) {
      return new JsonResponse<>(Status.FORBIDDEN, "No permission.").build();
    }
    HashMap<String, HashSet> permMap = gson.fromJson(req,
        new TypeToken<HashMap<String, HashSet>>() {
        }.getType());
    Note note = notebook.getNote(noteId);
    String principal = SecurityUtils.getPrincipal();
    LOG.info("Set permissions {} {} {} {} {}",
            noteId,
            principal,
            permMap.get("owners"),
            permMap.get("readers"),
            permMap.get("writers")
    );

    HashSet<String> userAndRoles = new HashSet<String>();
    userAndRoles.add(principal);
    userAndRoles.addAll(roles);
    if (!note.isOwner(userAndRoles)) {
      return new JsonResponse<>(Status.FORBIDDEN, ownerPermissionError(userAndRoles,
              note.getOwners())).build();
    }
    note.setOwners(permMap.get("owners"));
    note.setReaders(permMap.get("readers"));
    note.setWriters(permMap.get("writers"));
    LOG.debug("After set permissions {} {} {}", note.getOwners(), note.getReaders(),
            note.getWriters());
    note.persist();
    notebookServer.broadcastNote(note);
    return new JsonResponse<>(Status.OK).build();
  }

  /**
   * bind a setting to note
   * @throws IOException
   */
  @PUT
  @Path("interpreter/bind/{noteId}")
  public Response bind(@PathParam("noteId") String noteId, String req) throws IOException {
    HashSet<String> roles = SecurityUtils.getRoles();
    if (!(roles.contains("admin") || roles.contains("dev"))) {
      return new JsonResponse<>(Status.FORBIDDEN, "No permission to change interpreter").build();
    }
    List<String> settingIdList = gson.fromJson(req, new TypeToken<List<String>>(){}.getType());
    notebook.bindInterpretersToNote(noteId, settingIdList);
    return new JsonResponse<>(Status.OK).build();
  }

  /**
   * list binded setting
   */
  @GET
  @Path("interpreter/bind/{noteId}")
  public Response bind(@PathParam("noteId") String noteId) {
    HashSet<String> roles = SecurityUtils.getRoles();
    if (!(roles.contains("admin") || roles.contains("dev"))) {
      return new JsonResponse<>(Status.FORBIDDEN, "No permission to change interpreter").build();
    }
    List<InterpreterSettingListForNoteBind> settingList
      = new LinkedList<InterpreterSettingListForNoteBind>();

    List<InterpreterSetting> selectedSettings = notebook.getBindedInterpreterSettings(noteId);
    for (InterpreterSetting setting : selectedSettings) {
      settingList.add(new InterpreterSettingListForNoteBind(
          setting.id(),
          setting.getName(),
          setting.getGroup(),
          setting.getInterpreterInfos(),
          true)
      );
    }

    List<InterpreterSetting> availableSettings = notebook.getInterpreterFactory().get();
    for (InterpreterSetting setting : availableSettings) {
      boolean selected = false;
      for (InterpreterSetting selectedSetting : selectedSettings) {
        if (selectedSetting.id().equals(setting.id())) {
          selected = true;
          break;
        }
      }

      if (!selected) {
        settingList.add(new InterpreterSettingListForNoteBind(
            setting.id(),
            setting.getName(),
            setting.getGroup(),
            setting.getInterpreterInfos(),
            false)
        );
      }
    }
    return new JsonResponse<>(Status.OK, "", settingList).build();
  }

  @GET
  @Path("/")
  public Response getNotebookList() throws IOException {
    List<Map<String, String>> notesInfo = notebookServer.generateNotebooksInfo(false);
    return new JsonResponse<>(Status.OK, "", notesInfo ).build();
  }

  @GET
  @Path("{notebookId}")
  public Response getNotebook(@PathParam("notebookId") String notebookId) throws IOException {
    Note note = notebook.getNote(notebookId);
    if (note == null) {
      return new JsonResponse<>(Status.NOT_FOUND, "note not found.").build();
    }

    return new JsonResponse<>(Status.OK, "", note).build();
  }

  /**
   * export note REST API
   * 
   * @param
   * @return note JSON with status.OK
   * @throws IOException
   */
  @GET
  @Path("export/{id}")
  public Response exportNoteBook(@PathParam("id") String noteId) throws IOException {
    String exportJson = notebook.exportNote(noteId);
    return new JsonResponse(Status.OK, "", exportJson).build();
  }

  /**
   * import new note REST API
   * 
   * @param req - notebook Json
   * @return JSON with new note ID
   * @throws IOException
   */
  @POST
  @Path("import")
  public Response importNotebook(String req) throws IOException {
    Note newNote = notebook.importNote(req, null);
    return new JsonResponse<>(Status.CREATED, "", newNote.getId()).build();
  }
  
  /**
   * Create new note REST API
   * @param message - JSON with new note name
   * @return JSON with new note ID
   * @throws IOException
   */
  @POST
  @Path("/")
  public Response createNote(String message) throws IOException {
    LOG.info("Create new notebook by JSON {}" , message);
    NewNotebookRequest request = gson.fromJson(message,
        NewNotebookRequest.class);
    Note note = notebook.createNote();
    List<NewParagraphRequest> initialParagraphs = request.getParagraphs();
    if (initialParagraphs != null) {
      for (NewParagraphRequest paragraphRequest : initialParagraphs) {
        Paragraph p = note.addParagraph();
        p.setTitle(paragraphRequest.getTitle());
        p.setText(paragraphRequest.getText());
      }
    }
    note.addParagraph(); // add one paragraph to the last
    String noteName = request.getName();
    if (noteName.isEmpty()) {
      noteName = "Note " + note.getId();
    }
    note.setName(noteName);
    note.persist();
    notebookServer.broadcastNote(note);
    notebookServer.broadcastNoteList();
    return new JsonResponse<>(Status.CREATED, "", note.getId() ).build();
  }

  /**
   * Delete note REST API
   * @param
   * @return JSON with status.OK
   * @throws IOException
   */
  @DELETE
  @Path("{notebookId}")
  public Response deleteNote(@PathParam("notebookId") String notebookId) throws IOException {
    LOG.info("Delete notebook {} ", notebookId);
    if (!(notebookId.isEmpty())) {
      Note note = notebook.getNote(notebookId);
      if (note != null) {
        notebook.removeNote(notebookId);
      }
    }
    notebookServer.broadcastNoteList();
    return new JsonResponse<>(Status.OK, "").build();
  }
  
  /**
   * Clone note REST API
   * @param
   * @return JSON with status.CREATED
   * @throws IOException, CloneNotSupportedException, IllegalArgumentException
   */
  @POST
  @Path("{notebookId}")
  public Response cloneNote(@PathParam("notebookId") String notebookId, String message) throws
      IOException, CloneNotSupportedException, IllegalArgumentException {
    LOG.info("clone notebook by JSON {}" , message);
    NewNotebookRequest request = gson.fromJson(message,
        NewNotebookRequest.class);
    String newNoteName = request.getName();
    Note newNote = notebook.cloneNote(notebookId, newNoteName);
    notebookServer.broadcastNote(newNote);
    notebookServer.broadcastNoteList();
    return new JsonResponse<>(Status.CREATED, "", newNote.getId()).build();
  }

  /**
   * Insert paragraph REST API
   * @param message - JSON containing paragraph's information
   * @return JSON with status.OK
   * @throws IOException
   */
  @POST
  @Path("{notebookId}/paragraph")
  public Response insertParagraph(@PathParam("notebookId") String notebookId, String message)
      throws IOException {
    LOG.info("insert paragraph {} {}", notebookId, message);

    Note note = notebook.getNote(notebookId);
    if (note == null) {
      return new JsonResponse(Status.NOT_FOUND, "note not found.").build();
    }

    NewParagraphRequest request = gson.fromJson(message, NewParagraphRequest.class);

    Paragraph p;
    Double indexDouble = request.getIndex();
    if (indexDouble == null) {
      p = note.addParagraph();
    } else {
      p = note.insertParagraph(indexDouble.intValue());
    }
    p.setTitle(request.getTitle());
    p.setText(request.getText());

    note.persist();
    notebookServer.broadcastNote(note);
    return new JsonResponse(Status.CREATED, "", p.getId()).build();
  }

  /**
   * Get paragraph REST API
   * @param
   * @return JSON with information of the paragraph
   * @throws IOException
   */
  @GET
  @Path("{notebookId}/paragraph/{paragraphId}")
  public Response getParagraph(@PathParam("notebookId") String notebookId,
                               @PathParam("paragraphId") String paragraphId) throws IOException {
    LOG.info("get paragraph {} {}", notebookId, paragraphId);

    Note note = notebook.getNote(notebookId);
    if (note == null) {
      return new JsonResponse(Status.NOT_FOUND, "note not found.").build();
    }

    Paragraph p = note.getParagraph(paragraphId);
    if (p == null) {
      return new JsonResponse(Status.NOT_FOUND, "paragraph not found.").build();
    }

    return new JsonResponse(Status.OK, "", p).build();
  }

  /**
   * Move paragraph REST API
   * @param newIndex - new index to move
   * @return JSON with status.OK
   * @throws IOException
   */
  @POST
  @Path("{notebookId}/paragraph/{paragraphId}/move/{newIndex}")
  public Response moveParagraph(@PathParam("notebookId") String notebookId,
                                @PathParam("paragraphId") String paragraphId,
                                @PathParam("newIndex") String newIndex) throws IOException {
    LOG.info("move paragraph {} {} {}", notebookId, paragraphId, newIndex);

    Note note = notebook.getNote(notebookId);
    if (note == null) {
      return new JsonResponse(Status.NOT_FOUND, "note not found.").build();
    }

    Paragraph p = note.getParagraph(paragraphId);
    if (p == null) {
      return new JsonResponse(Status.NOT_FOUND, "paragraph not found.").build();
    }

    try {
      note.moveParagraph(paragraphId, Integer.parseInt(newIndex), true);

      note.persist();
      notebookServer.broadcastNote(note);
      return new JsonResponse(Status.OK, "").build();
    } catch (IndexOutOfBoundsException e) {
      LOG.error("Exception in NotebookRestApi while moveParagraph ", e);
      return new JsonResponse(Status.BAD_REQUEST, "paragraph's new index is out of bound").build();
    }
  }

  /**
   * Delete paragraph REST API
   * @param
   * @return JSON with status.OK
   * @throws IOException
   */
  @DELETE
  @Path("{notebookId}/paragraph/{paragraphId}")
  public Response deleteParagraph(@PathParam("notebookId") String notebookId,
                                  @PathParam("paragraphId") String paragraphId) throws IOException {
    LOG.info("delete paragraph {} {}", notebookId, paragraphId);

    Note note = notebook.getNote(notebookId);
    if (note == null) {
      return new JsonResponse(Status.NOT_FOUND, "note not found.").build();
    }

    Paragraph p = note.getParagraph(paragraphId);
    if (p == null) {
      return new JsonResponse(Status.NOT_FOUND, "paragraph not found.").build();
    }

    note.removeParagraph(paragraphId);
    note.persist();
    notebookServer.broadcastNote(note);

    return new JsonResponse(Status.OK, "").build();
  }

  /**
   * Download paragraph
   * @param
   * @return data stream
   * @throws IOException
   */
  @GET
  @Produces("text/csv")
  @Path("{notebookId}/paragraph/{paragraphId}/download")
  public Response downParagraph(@PathParam("notebookId") String notebookId,
                                  @PathParam("paragraphId") String paragraphId)
      throws IOException {
    LOG.info("Download paragraph, node.id: {}, paragrap.id: {}", notebookId, paragraphId);

    Note note = notebook.getNote(notebookId);
    if (note == null) {
      return new JsonResponse(Status.NOT_FOUND, "note not found.").build();
    }

    Paragraph p = note.getParagraph(paragraphId);
    if (p == null) {
      return new JsonResponse(Status.NOT_FOUND, "paragraph not found.").build();
    }

    String resultFileParent = conf.getString(ConfVars.ZEPPELIN_RESULT_DATA_DIR);

    String fileName = notebookId + "_" + paragraphId;
    final File file = new File(resultFileParent + "/" + fileName);

    ResponseBuilder response = null;
    if (file.exists()) {
      LOG.info("Download data from " + file);
      response = Response.ok((Object) file);
    } else {
      LOG.info("Download data from paragraph result " +
          "because result file is not exists: " + file);
      response = Response.ok(p.getResultMessage());
    }
    response.header("Content-Disposition", "attachment; filename=\"" + fileName + ".csv\"");
    response.type(MediaType.TEXT_PLAIN + "; charset=UTF-8");
    return response.build();
  }

  /**
   * Run notebook jobs REST API
   * @param
   * @return JSON with status.OK
   * @throws IOException, IllegalArgumentException
   */
  @POST
  @Path("job/{notebookId}")
  public Response runNoteJobs(@PathParam("notebookId") String notebookId) throws
      IOException, IllegalArgumentException {
    LOG.info("run notebook jobs {} ", notebookId);
    Note note = notebook.getNote(notebookId);
    if (note == null) {
      return new JsonResponse<>(Status.NOT_FOUND, "note not found.").build();
    }
    
    note.runAll();
    return new JsonResponse<>(Status.OK).build();
  }

  /**
   * Stop(delete) notebook jobs REST API
   * @param
   * @return JSON with status.OK
   * @throws IOException, IllegalArgumentException
   */
  @DELETE
  @Path("job/{notebookId}")
  public Response stopNoteJobs(@PathParam("notebookId") String notebookId) throws
      IOException, IllegalArgumentException {
    LOG.info("stop notebook jobs {} ", notebookId);
    Note note = notebook.getNote(notebookId);
    if (note == null) {
      return new JsonResponse<>(Status.NOT_FOUND, "note not found.").build();
    }

    for (Paragraph p : note.getParagraphs()) {
      if (!p.isTerminated()) {
        p.abort();
      }
    }
    return new JsonResponse<>(Status.OK).build();
  }
  
  /**
   * Get notebook job status REST API
   * @param
   * @return JSON with status.OK
   * @throws IOException, IllegalArgumentException
   */
  @GET
  @Path("job/{notebookId}")
  public Response getNoteJobStatus(@PathParam("notebookId") String notebookId) throws
      IOException, IllegalArgumentException {
    LOG.info("get notebook job status.");
    Note note = notebook.getNote(notebookId);
    if (note == null) {
      return new JsonResponse<>(Status.NOT_FOUND, "note not found.").build();
    }

    return new JsonResponse<>(Status.OK, null, note.generateParagraphsInfo()).build();
  }
  
  /**
   * Run paragraph job REST API
   * 
   * @param message - JSON with params if user wants to update dynamic form's value
   *                null, empty string, empty json if user doesn't want to update
   *
   * @return JSON with status.OK
   * @throws IOException, IllegalArgumentException
   */
  @POST
  @Path("job/{notebookId}/{paragraphId}")
  public Response runParagraph(@PathParam("notebookId") String notebookId, 
                               @PathParam("paragraphId") String paragraphId,
                               String message) throws
                               IOException, IllegalArgumentException {
    LOG.info("run paragraph job {} {} {}", notebookId, paragraphId, message);

    Note note = notebook.getNote(notebookId);
    if (note == null) {
      return new JsonResponse<>(Status.NOT_FOUND, "note not found.").build();
    }

    Paragraph paragraph = note.getParagraph(paragraphId);
    if (paragraph == null) {
      return new JsonResponse<>(Status.NOT_FOUND, "paragraph not found.").build();
    }

    // handle params if presented
    if (!StringUtils.isEmpty(message)) {
      RunParagraphWithParametersRequest request = gson.fromJson(message,
          RunParagraphWithParametersRequest.class);
      Map<String, Object> paramsForUpdating = request.getParams();
      if (paramsForUpdating != null) {
        paragraph.settings.getParams().putAll(paramsForUpdating);
        note.persist();
      }
    }

    note.run(paragraph.getId());
    return new JsonResponse<>(Status.OK).build();
  }

  /**
   * Stop(delete) paragraph job REST API
   * @param
   * @return JSON with status.OK
   * @throws IOException, IllegalArgumentException
   */
  @DELETE
  @Path("job/{notebookId}/{paragraphId}")
  public Response stopParagraph(@PathParam("notebookId") String notebookId, 
                                @PathParam("paragraphId") String paragraphId) throws
                                IOException, IllegalArgumentException {
    LOG.info("stop paragraph job {} ", notebookId);
    Note note = notebook.getNote(notebookId);
    if (note == null) {
      return new JsonResponse<>(Status.NOT_FOUND, "note not found.").build();
    }

    Paragraph p = note.getParagraph(paragraphId);
    if (p == null) {
      return new JsonResponse<>(Status.NOT_FOUND, "paragraph not found.").build();
    }
    p.abort();
    return new JsonResponse<>(Status.OK).build();
  }
    
  /**
   * Register cron job REST API
   * @param message - JSON with cron expressions.
   * @return JSON with status.OK
   * @throws IOException, IllegalArgumentException
   */
  @POST
  @Path("cron/{notebookId}")
  public Response registerCronJob(@PathParam("notebookId") String notebookId, String message) throws
      IOException, IllegalArgumentException {
    LOG.info("Register cron job note={} request cron msg={}", notebookId, message);

    CronRequest request = gson.fromJson(message,
                          CronRequest.class);
    
    Note note = notebook.getNote(notebookId);
    if (note == null) {
      return new JsonResponse<>(Status.NOT_FOUND, "note not found.").build();
    }
    
    if (!CronExpression.isValidExpression(request.getCronString())) {
      return new JsonResponse<>(Status.BAD_REQUEST, "wrong cron expressions.").build();
    }

    Map<String, Object> config = note.getConfig();
    config.put("cron", request.getCronString());
    note.setConfig(config);
    notebook.refreshCron(note.id());
    
    return new JsonResponse<>(Status.OK).build();
  }
  
  /**
   * Remove cron job REST API
   * @param
   * @return JSON with status.OK
   * @throws IOException, IllegalArgumentException
   */
  @DELETE
  @Path("cron/{notebookId}")
  public Response removeCronJob(@PathParam("notebookId") String notebookId) throws
      IOException, IllegalArgumentException {
    LOG.info("Remove cron job note {}", notebookId);

    Note note = notebook.getNote(notebookId);
    if (note == null) {
      return new JsonResponse<>(Status.NOT_FOUND, "note not found.").build();
    }
    
    Map<String, Object> config = note.getConfig();
    config.put("cron", null);
    note.setConfig(config);
    notebook.refreshCron(note.id());
    
    return new JsonResponse<>(Status.OK).build();
  }  
  
  /**
   * Get cron job REST API
   * @param
   * @return JSON with status.OK
   * @throws IOException, IllegalArgumentException
   */
  @GET
  @Path("cron/{notebookId}")
  public Response getCronJob(@PathParam("notebookId") String notebookId) throws
      IOException, IllegalArgumentException {
    LOG.info("Get cron job note {}", notebookId);

    Note note = notebook.getNote(notebookId);
    if (note == null) {
      return new JsonResponse<>(Status.NOT_FOUND, "note not found.").build();
    }
    
    return new JsonResponse<>(Status.OK, note.getConfig().get("cron")).build();
  }  

  /**
   * Search for a Notes
   */
  @GET
  @Path("search")
  public Response search(@QueryParam("q") String queryTerm) {
    LOG.info("Searching notebooks for: {}", queryTerm);
    List<Map<String, String>> notebooksFound = notebookIndex.query(queryTerm);
    LOG.info("{} notbooks found", notebooksFound.size());
    return new JsonResponse<>(Status.OK, notebooksFound).build();
  }

}

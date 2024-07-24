#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

#ifdef HAS_XMOTIF

#include <signal.h>

#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "memmap.h"

#include <Xm/Xm.h>
#include <Xm/Text.h>
#include <Xm/Form.h>
#include <Xm/PushB.h>
#include <Xm/ToggleB.h>
#include <Xm/RowColumn.h>
#include <Xm/CascadeB.h>
#include <Xm/FileSB.h>
#include <Xm/MessageB.h>
#include <Xm/SelectioB.h>

#define NAL 20

/* integer values used to distinguish the call to menuCB. */
#define MENU_FILE_OPEN     (XtPointer)11
#define MENU_FILE_QUIT     (XtPointer)13

#define MENU_EDIT_SEARCH   (XtPointer)21

#define MENU_FONT_DEFAULT  (XtPointer)31
#define MENU_FONT_FIXED    (XtPointer)32
#define MENU_FONT_SMALL    (XtPointer)33
#define MENU_FONT_MEDIUM   (XtPointer)34
#define MENU_FONT_LARGE    (XtPointer)35

#define MENU_PLOT_COVERAGE  (XtPointer)41
#define MENU_PLOT_XY        (XtPointer)42

/* integer values used to distinguish the call to dialogCBs. */
#define OK            (XtPointer)1
#define CANCEL        (XtPointer)2

XtAppContext context;
XmStringCharSet char_set=XmSTRING_DEFAULT_CHARSET;

/* all widgets are global to make life easier. */
Widget toplevel, text, form, label, menu_bar;
Widget open_option, plot_coverage_option, quit_option;
Widget search_option, plot_xy_option;
Widget open_dialog, search_dialog;
Widget font_default_option, font_fixed_option;
Widget font_small_option;
Widget font_medium_option, font_large_option;
Widget plot_coverage_dialog, plot_xy_dialog;

char *fontname_default = "*lucidasanstypewriter-18*";
char *fontname_fixed = "fixed";
char *fontname_small = "*lucidasanstypewriter-bold-14*";
char *fontname_medium = "*lucidasanstypewriter-bold-18*";
char *fontname_large = "*lucidasanstypewriter-bold-24*";

char *filename = NULL;
char *file_contents = NULL;
char *search_ptr = NULL;

#define ALLOC(x,size)   { \
  x = malloc(sizeof(*x) * (1 + (size))); \
  if (!x) { fprintf(stderr,"***Error: Unable to malloc() %d bytes\n",\
                    sizeof(*x) * (1 + (size))); raise(SIGABRT); } }
#define FREE(x) { if ((x)) {free((x)); (x) = NULL;} }

#undef MIN
#define MIN(a,b) ( ((a) < (b)) ? (a) : (b) )

#undef MAX
#define MAX(a,b) ( ((a) > (b)) ? (a) : (b) )

static size_t 
filesize(FILE *fp)
{
  struct stat buf;
  size_t file_size = 0;
  if (fstat(fileno(fp),&buf) != 0) {
    file_size = -1;
  }
  else {
    file_size = buf.st_size;
  }
  return file_size;
}


static void
pass_input()
{
  if (filename) {
    FILE *fp = fopen(filename,"r");
    size_t lenp = 0;
    char *p = NULL;
    int len_iobuf = 1048576;
    char *iobuf = NULL;
    char *env = getenv("ODB_REPORTER_MAXBYTES");
    int maxbytes = 536870912; /* i.e.  512 MBytes : read max this many bytes from file to avoid paging */
    const char trunc_msg[] = "\n\n\n\n.....(this file has been truncated; Increase max. size via ODB_REPORTER_MAXBYTES).....\n";
    char *env_mmap = getenv("ODB_REPORTER_MMAP");
    int use_mmap = env_mmap ? atoi(env_mmap) : 1;
    int trunc_len = 0;
    static memmap_t *m = NULL;
    static FILE *oldfp = NULL;
    int fd = 0;

    if (!fp) {
      perror(filename);
      exit(1);
    }

    if (env) {
      int ienv = atoi(env);
      if (ienv >= 0) maxbytes = ienv;
      else if (ienv == -1) {
	maxbytes = 2147483647;
      }
    }

    if (m) {
#if 0
      fprintf(stderr,"Closing memory mapped file: %p %d\n",m->buf,m->len);
#endif
      (void) memmap_close(m);
      m = NULL;
      if (oldfp) {
	fclose(oldfp);
	oldfp = NULL;
      }
      file_contents = NULL;
    }
    else if (file_contents) {
      FREE(file_contents);
      search_ptr = NULL;
    }

    lenp = filesize(fp);
    if (lenp > maxbytes) {
      fprintf(stderr,
	      "***Warning: Only the first %d bytes will be read from file '%s', length %d bytes\n",
	      maxbytes, filename, lenp);
      fprintf(stderr,
	      "            Can be increased via ODB_REPORTER_MAXBYTES\n");
      lenp = maxbytes;
      trunc_len = strlen(trunc_msg);
    }

    fd = fileno(fp);

    if (use_mmap) {
      m = memmap_open_read(fd, NULL, &lenp);
    }
    else { /* Don't want memory mapped I/O */
      m = NULL;
    }

    if (m) {
      oldfp = fp;
      p = (char *)m->buf;
      lenp = m->len;
#if 0
      fprintf(stderr,"Using memory mapped file: %p %d\n",p,lenp);
#endif
      /* We leave the file open */
    }
    else {
      ALLOC(p, lenp + trunc_len + 1);
      
      len_iobuf = MIN(len_iobuf, lenp);
      ALLOC(iobuf, len_iobuf);
      setvbuf(fp, iobuf, _IOFBF, len_iobuf);
    
      fread(p, sizeof(*p), lenp, fp);
      if (trunc_len > 0) memcpy(&p[lenp],trunc_msg,trunc_len);
      p[lenp+trunc_len] = '\0';
      fclose(fp);

      FREE(iobuf);
    }
    
    XmTextSetString(text, p);

    file_contents = p;
    
    XtSetSensitive(text,True);
    XmTextSetEditable(text,False);
    XmTextSetCursorPosition(text,0);
    
    FREE(filename);

    XtSetSensitive(search_option,True);
  }
}

void
change_font(Widget w, char *fn)
{
  int ac;
  Arg al[NAL];
  XFontStruct *font = NULL;
  XmFontList fontlist = NULL;

  if (!fn) fn = fontname_default;

  font = XLoadQueryFont(XtDisplay(w), fn);
  fontlist = XmFontListCreate(font, char_set);
  ac = 0;
  XtSetArg(al[ac], XmNfontList,fontlist); ac++;
  XtSetValues(w, al, ac);
}

void
openCB(Widget w,
       XtPointer client_data,
       XmAnyCallbackStruct *call_data)
     /* handles the file selection box callbacks. */
{
  XmFileSelectionBoxCallbackStruct *s =
      (XmFileSelectionBoxCallbackStruct *) call_data;

  if (client_data==CANCEL) {
    /* do nothing if cancel is selected. */
    XtUnmanageChild(open_dialog);
    return;
  }

  if (filename != NULL) {
    /* free up filename if it exists. */
    XtFree(filename);
    filename = NULL;
  }

  /* get the filename from the file selection box */
  XmStringGetLtoR(s->value, char_set, &filename);

  /* Read file and pass input to the window */

  pass_input();

  XtUnmanageChild(open_dialog);
}


void
plotCB(Widget w,
       XtPointer client_data,
       XmAnyCallbackStruct *call_data)
     /* handles the file selection box callbacks. */
{
  XmFileSelectionBoxCallbackStruct *s =
      (XmFileSelectionBoxCallbackStruct *) call_data;

  if (client_data==CANCEL) {
    /* do nothing if cancel is selected. */
    XtUnmanageChild(plot_coverage_dialog);
    return;
  }

  if (filename != NULL) {
    /* free up filename if it exists. */
    XtFree(filename);
    filename = NULL;
  }

  /* get the filename from the file selection box */
  XmStringGetLtoR(s->value, char_set, &filename);

  /* Use ghostview to plot the postscript file */

  {
    int len = 10;
    char *cmd;
    char *env = getenv("ODB_DISPLAY_PLOT");
    /* if (!env) env = "ghostview -land"; */
    if (!env) env = "xv";
    len += strlen(env);
    len += strlen(filename);
    ALLOC(cmd, len);
    sprintf(cmd, "%s %s &",env, filename);
    system(cmd);
    FREE(cmd);
  }

  XtUnmanageChild(plot_coverage_dialog);
}


void
xyplotCB(Widget w,
       XtPointer client_data,
       XmAnyCallbackStruct *call_data)
     /* handles the file selection box callbacks. */
{
  XmFileSelectionBoxCallbackStruct *s =
      (XmFileSelectionBoxCallbackStruct *) call_data;

  if (client_data==CANCEL) {
    /* do nothing if cancel is selected. */
    XtUnmanageChild(plot_xy_dialog);
    return;
  }

  if (filename != NULL) {
    /* free up filename if it exists. */
    XtFree(filename);
    filename = NULL;
  }

  /* get the filename from the file selection box */
  XmStringGetLtoR(s->value, char_set, &filename);

  /* Use ghostview to plot the postscript file */

  {
    int len = 50;
    char *cmd;
    char *prog;
    char *cx, *cy, *ct, *dev;
    char *env = getenv("ODB_XY_PLOTTER");

    prog = env ? strdup(env) : strdup("odbxyplot");

    len += strlen(prog);
    len += strlen(filename);

    env = getenv("ODB_XY_COL_X");
    cx = env ? strdup(env) : strdup("1");

    env = getenv("ODB_XY_COL_Y");
    cy = env ? strdup(env) : strdup("2");

    env = getenv("ODB_XY_COL_T");
    ct = env ? strdup(env) : strdup("0");

    env = getenv("ODB_XY_DEVICE");
    dev = env ? strdup(env) : strdup("x11");

    len += strlen(cx) + strlen(cy) + strlen(ct) + strlen(dev);

    ALLOC(cmd, len);
    sprintf(cmd, "%s -x%s -y%s -t%s -d%s %s &", prog, cx, cy, ct, dev, filename);
    FREE(prog);
    FREE(cx); 
    FREE(cy);
    FREE(ct);
    FREE(dev);
    /* fprintf(stderr,"Running: %s\n",cmd); */
    system(cmd);
    FREE(cmd);
  }

  XtUnmanageChild(plot_xy_dialog);
}


void
searchCB(Widget w,
	 XtPointer client_data,
	 XmSelectionBoxCallbackStruct *call_data)
     /* callback function for the search box */
{
  char *s;
  int lens;
  static int hlpos[2] = { 0, -1 };

  if (hlpos[1] >= hlpos[0]) {
    XmTextSetHighlight(text,hlpos[0],hlpos[1],XmHIGHLIGHT_NORMAL);
    hlpos[0] =  0;
    hlpos[1] = -1;
  }

  if (client_data == OK) {
    /* get the string from the event structure. */
    XmStringGetLtoR(call_data->value,char_set,&s);
    /* printf("Searching for '%s' ...\n",s); */

    lens = strlen(s);

    if (!search_ptr) {
      int curpos = XmTextGetCursorPosition(text);
      search_ptr = file_contents + curpos;
    }

    search_ptr = strstr(search_ptr, s);
    if (search_ptr) {
      int newpos = (int)(search_ptr-file_contents) + lens;
      XmTextSetCursorPosition(text,newpos);
      hlpos[0] = newpos-lens;
      hlpos[1] = newpos;
      XmTextSetHighlight(text,hlpos[0],hlpos[1],XmHIGHLIGHT_SELECTED);
      search_ptr++;
    }
    else {
      /* printf("'%s' not found\n",s); */
      XmTextSetCursorPosition(text,0);
    }

    XtFree(s);
    /* XtManageChild(w); */
  }
  else if (client_data == CANCEL) {
    /* printf("CANCEL selected\n"); */
    /* make the dialog box invisible */
    XtUnmanageChild(w);
  }
}


void
menuCB(Widget w,
       XtPointer client_data,
       XmAnyCallbackStruct *call_data)
/* handles menu options. */
{
  if (client_data==MENU_FILE_OPEN) {
    /* make the file selection box to appear */
    XtManageChild(open_dialog);
  }
  else if (client_data==MENU_FILE_QUIT) {
    exit(0);
  }
  else if (client_data==MENU_EDIT_SEARCH) {
    XtManageChild(search_dialog);
  }
  else if (client_data==MENU_FONT_DEFAULT) {
    /* selects the default font */
    change_font(text, NULL);
  }
  else if (client_data==MENU_FONT_FIXED) {
    /* selects the fixed font */
    change_font(text, fontname_fixed);
  }
  else if (client_data==MENU_FONT_SMALL) {
    /* selects the small font */
    change_font(text, fontname_small);
  }
  else if (client_data==MENU_FONT_MEDIUM) {
    /* selects the medium font */
    change_font(text, fontname_medium);
  }
  else if (client_data==MENU_FONT_LARGE) {
    /* selects the large font */
    change_font(text, fontname_large);
  }
  else if (client_data==MENU_PLOT_COVERAGE) {
    /* make the plot file selection box to appear */
    XtManageChild(plot_coverage_dialog);
  }
  else if (client_data==MENU_PLOT_XY) {
    /* make the xy-plot file selection box to appear */
    XtManageChild(plot_xy_dialog);
  }
}

Widget 
make_menu_option(char *option_name,
		 XtPointer client_data,
		 Widget menu)
{
  int ac;
  Arg al[NAL];
  Widget b;

  ac = 0;
  XtSetArg(al[ac], XmNlabelString,
	   XmStringCreateLtoR(option_name,char_set)); ac++;
  b=XmCreatePushButton(menu,option_name,al,ac);
  XtManageChild(b);

  XtAddCallback (b, XmNactivateCallback, (XtCallbackProc)menuCB, client_data);
  return(b);
}

Widget 
make_menu_toggle(char *option_name,
		 XtPointer client_data,
		 Widget menu,
		 Boolean set)
{
  int ac;
  Arg al[NAL];
  Widget b;

  ac = 0;
  XtSetArg(al[ac], XmNlabelString,
	   XmStringCreateLtoR(option_name,char_set)); ac++;
  XtSetArg(al[ac], XmNset, set); ac++;
  b=XmCreateToggleButton(menu,option_name,al,ac);
  XtManageChild(b);

  XtAddCallback (b, XmNvalueChangedCallback, (XtCallbackProc)menuCB, client_data);
  XtSetSensitive(b,True);
  return(b);
}


Widget 
make_menu(char *menu_name,
	  Widget menu_bar)
{
  int ac;
  Arg al[NAL];
  Widget menu, cascade;
  
  ac = 0;
  menu = XmCreatePulldownMenu (menu_bar, menu_name, al, ac);
  
  ac = 0;
  XtSetArg (al[ac], XmNsubMenuId, menu);  ac++;
  XtSetArg(al[ac], XmNlabelString,
	   XmStringCreateLtoR(menu_name, char_set)); ac++;
  cascade = XmCreateCascadeButton (menu_bar, menu_name, al, ac);

  XtManageChild (cascade);
  
  return(menu);
}


void 
create_menus(Widget menu_bar)
{
  int ac;
  Arg al[NAL];
  Widget menu;
  
  menu=make_menu("File",menu_bar);
  open_option = make_menu_option("Open",MENU_FILE_OPEN,menu);
  quit_option = make_menu_option("Quit",MENU_FILE_QUIT,menu);

  menu=make_menu("Edit",menu_bar);
  search_option = make_menu_option("Search",MENU_EDIT_SEARCH,menu);
  XtSetSensitive(search_option,False);

  menu=make_menu("TextSize",menu_bar);
  font_default_option = make_menu_toggle("Default",MENU_FONT_DEFAULT,menu,1);
  font_fixed_option = make_menu_toggle("Fixed",MENU_FONT_FIXED,menu,0);
  font_small_option = make_menu_toggle("Small",MENU_FONT_SMALL,menu,0);
  font_medium_option = make_menu_toggle("Medium",MENU_FONT_MEDIUM,menu,0);
  font_large_option = make_menu_toggle("Large",MENU_FONT_LARGE,menu,0);
  ac=0;
  XtSetArg(al[ac], XmNradioBehavior,True); ac++;
  /* XtSetArg(al[ac], XmNradioAlwaysOne, True); ac++; */
  XtSetValues(menu,al,ac);

  menu = make_menu("Plot",menu_bar);
  plot_coverage_option = make_menu_option("Coverage plot",MENU_PLOT_COVERAGE,menu);
  plot_xy_option       = make_menu_option("XY-plot",MENU_PLOT_XY,menu);
}


int
main(int argc, char *argv[])
{
  Arg al[NAL];
  int ac;

  /* create the toplevel shell */
  toplevel = XtAppInitialize(&context,"",NULL,0,
			     &argc,argv,NULL,NULL,0);

  if (argc > 1) filename = strdup(argv[1]);
  
  /* default window size. */
  /*
  ac=0;
  XtSetArg(al[ac],XmNheight,200); ac++;
  XtSetArg(al[ac],XmNwidth,200); ac++;
  XtSetValues(toplevel,al,ac);
  */

  /* create a form widget. */
  ac=0;
  if (argc > 1) {
    XtSetArg(al[ac], XmNdialogTitle, XmStringCreateLtoR(filename, char_set));
    ac++;
  }
  form=XmCreateForm(toplevel,"form",al,ac);
  XtManageChild(form);

  /* create a menu bar and attach it to the form. */
  ac=0;
  XtSetArg(al[ac], XmNtopAttachment,   XmATTACH_FORM); ac++;
  XtSetArg(al[ac], XmNrightAttachment, XmATTACH_FORM); ac++;
  XtSetArg(al[ac], XmNleftAttachment,  XmATTACH_FORM); ac++;
  menu_bar=XmCreateMenuBar(form,"menu_bar",al,ac);
  XtManageChild(menu_bar);

  /* create a text widget and attach it to the form. */
  ac=0;
  XtSetArg(al[ac], XmNtopAttachment,    XmATTACH_WIDGET); ac++;
  XtSetArg(al[ac], XmNtopWidget, menu_bar); ac++;
  XtSetArg(al[ac], XmNrightAttachment, XmATTACH_FORM); ac++;
  XtSetArg(al[ac], XmNleftAttachment,   XmATTACH_FORM); ac++;
  XtSetArg(al[ac], XmNbottomAttachment,   XmATTACH_FORM); ac++;
  XtSetArg(al[ac], XmNeditMode,XmMULTI_LINE_EDIT); ac++;
  XtSetArg(al[ac], XmNeditable, FALSE); ac++;
  XtSetArg(al[ac], XmNcolumns, 100); ac++;
  XtSetArg(al[ac], XmNrows, 30); ac++;

  text=XmCreateScrolledText(form, "text", al, ac);
  /* XtAddCallback (text, XmNvalueChangedCallback, (XtCallbackProc)changedCB, NULL); */

  change_font(text, NULL);

  XtManageChild(text);

  /* Create menus */

  create_menus(menu_bar);

  /* create the file selection box used by open option. */
  ac=0;
  XtSetArg(al[ac],XmNmustMatch,True); ac++;
  XtSetArg(al[ac],XmNautoUnmanage,False); ac++;
  open_dialog=XmCreateFileSelectionDialog(toplevel,"open_dialog",al,ac);
  XtAddCallback (open_dialog, XmNokCallback,(XtCallbackProc)openCB,OK);
  XtAddCallback (open_dialog, XmNcancelCallback,(XtCallbackProc)openCB,CANCEL);
  XtUnmanageChild(XmSelectionBoxGetChild(open_dialog,
					 XmDIALOG_HELP_BUTTON));

  /* create the file selection box used by coverage plot option. */
  ac=0;
  XtSetArg(al[ac],XmNmustMatch,True); ac++;
  XtSetArg(al[ac],XmNautoUnmanage,False); ac++;
  XtSetArg(al[ac],XmNpattern,
	   XmStringCreateLtoR("*.jpg", char_set)); ac++;
  plot_coverage_dialog=XmCreateFileSelectionDialog(toplevel,"plot_coverage_dialog",al,ac);
  XtAddCallback (plot_coverage_dialog, XmNokCallback,(XtCallbackProc)plotCB,OK);
  XtAddCallback (plot_coverage_dialog, XmNcancelCallback,(XtCallbackProc)plotCB,CANCEL);
  XtUnmanageChild(XmSelectionBoxGetChild(plot_coverage_dialog,
					 XmDIALOG_HELP_BUTTON));

  /* create the file selection box used by XY-plot option. */
  ac=0;
  XtSetArg(al[ac],XmNmustMatch,True); ac++;
  XtSetArg(al[ac],XmNautoUnmanage,False); ac++;
  XtSetArg(al[ac],XmNpattern,
	   XmStringCreateLtoR("*.rpt", char_set)); ac++;
  plot_xy_dialog=XmCreateFileSelectionDialog(toplevel,"plot_xy_dialog",al,ac);
  XtAddCallback (plot_xy_dialog, XmNokCallback,(XtCallbackProc)xyplotCB,OK);
  XtAddCallback (plot_xy_dialog, XmNcancelCallback,(XtCallbackProc)xyplotCB,CANCEL);
  XtUnmanageChild(XmSelectionBoxGetChild(plot_xy_dialog,
					 XmDIALOG_HELP_BUTTON));

  /* create the search_dialog box. */
  ac=0;
  XtSetArg(al[ac],XmNautoUnmanage,False); ac++;
  XtSetArg(al[ac], XmNselectionLabelString,
	   XmStringCreateLtoR("Search for ",char_set)); ac++;
  search_dialog = XmCreatePromptDialog(toplevel,"search_dialog",al,ac);
  XtAddCallback(search_dialog,XmNokCallback,(XtCallbackProc)searchCB,OK);
  XtAddCallback(search_dialog,XmNcancelCallback,(XtCallbackProc)searchCB,CANCEL);
  XtUnmanageChild(XmSelectionBoxGetChild(search_dialog,
					 XmDIALOG_HELP_BUTTON));

  pass_input();

  XtRealizeWidget(toplevel);
  XtAppMainLoop(context);
}

/* For example: cc -o b4 b4.c -lXm -lXt -lX11 */

#else
/* no XMOTIF */

#if !defined(HEAD)
#define HEAD "/usr/bin/head"
#endif /* if !defined(HEAD) */

int
main(int argc, char *argv[])
{
  int rc = 0;
  if (argc > 1 && (access(HEAD,X_OK) == 0)) { 
    /* Display up to the first 10 lines of the files using head-command */
    int j;
    const int n = 10;
    for (j=1; j<argc; j++) {
      if (access(argv[j],R_OK) == 0) {
	/* char cmd[sizeof(HEAD) + strlen(argv[j]) + 20]; */
	int len = sizeof(HEAD) + strlen(argv[j]) + 20;
	char *cmd = malloc(len);
	snprintf(cmd,len,"%s -%d %s",HEAD,n,argv[j]);
	printf("\n\tThe first %d lines of the file '%s' :\n\n",n,argv[j]);
	fflush(stdout);
	system(cmd);
	fflush(stdout);
	printf("\n\n\tWant to see the full output ? Please look at the file '%s'\n\n",argv[j]);
	fflush(stdout);
	free(cmd);
      }
      else {
	fprintf(stderr,
		"***Error: File '%s' is not accessible for reading!!\n",argv[j]);
	rc++;
      }
    }
  }
  else {
    fprintf(stderr,
	    "***Error: Executable %s was build with X-Motif disabled\n",argv[0]);
    rc = 1;
  }
  return rc;
}

#endif

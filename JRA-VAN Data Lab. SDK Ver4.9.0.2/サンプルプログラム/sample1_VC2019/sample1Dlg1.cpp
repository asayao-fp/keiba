//========================================================================
//	JRA-VAN Data Lab. サンプルプログラム１(Sample1Dlg1.cpp)
//
//
//	 作成: JRA-VAN ソフトウェア工房  2003年4月22日
//
//========================================================================
//	 (C) Copyright Turf Media System Co.,Ltd. 2003 All rights reserved
//========================================================================

#include "stdafx.h"
#include "sample1.h"
#include "sample1Dlg1.h"
#include "sample1Dlg2.h"
#include "sample1Del.h"

#ifdef _DEBUG
#define new DEBUG_NEW
#undef THIS_FILE
static char THIS_FILE[] = __FILE__;
#endif

/////////////////////////////////////////////////////////////////////////////
// アプリケーションのバージョン情報で使われている CAboutDlg ダイアログ


class CAboutDlg : public CDialog
{
public:
	CAboutDlg();

// ダイアログ データ
	//{{AFX_DATA(CAboutDlg)
	enum { IDD = IDD_ABOUTBOX };
	//}}AFX_DATA

	// ClassWizard は仮想関数のオーバーライドを生成します
	//{{AFX_VIRTUAL(CAboutDlg)
	protected:
	virtual void DoDataExchange(CDataExchange* pDX);    // DDX/DDV のサポート
	//}}AFX_VIRTUAL

// インプリメンテーション
protected:
	//{{AFX_MSG(CAboutDlg)
	//}}AFX_MSG
	DECLARE_MESSAGE_MAP()
};

CAboutDlg::CAboutDlg() : CDialog(CAboutDlg::IDD)
{
	//{{AFX_DATA_INIT(CAboutDlg)
	//}}AFX_DATA_INIT
}

void CAboutDlg::DoDataExchange(CDataExchange* pDX)
{
	CDialog::DoDataExchange(pDX);
	//{{AFX_DATA_MAP(CAboutDlg)
	//}}AFX_DATA_MAP
}

BEGIN_MESSAGE_MAP(CAboutDlg, CDialog)
	//{{AFX_MSG_MAP(CAboutDlg)
		// メッセージ ハンドラがありません。
	//}}AFX_MSG_MAP
END_MESSAGE_MAP()


/////////////////////////////////////////////////////////////////////////////
// CSample1Dlg1 ダイアログ

//メインフォーム用のポインタ変数
CSample1Dlg1* m_pView;

CSample1Dlg1::CSample1Dlg1(CWnd* pParent /*=NULL*/)
	: CDialog(CSample1Dlg1::IDD, pParent)
{
	//{{AFX_DATA_INIT(CSample1Dlg1)
	//}}AFX_DATA_INIT
	// メモ: LoadIcon は Win32 の DestroyIcon のサブシーケンスを要求しません。
	m_hIcon = AfxGetApp()->LoadIcon(IDR_MAINFRAME);
}

void CSample1Dlg1::DoDataExchange(CDataExchange* pDX)
{
	CDialog::DoDataExchange(pDX);
	//{{AFX_DATA_MAP(CSample1Dlg1)
	DDX_Control(pDX, IDC_RICHEDIT1, m_strOut);
	DDX_Control(pDX, IDC_RICHEDIT2, m_strFileList);
	DDX_Control(pDX, IDC_JVLINK1, m_jvlink1);
	//}}AFX_DATA_MAP
}

BEGIN_MESSAGE_MAP(CSample1Dlg1, CDialog)
	//{{AFX_MSG_MAP(CSample1Dlg1)
	ON_WM_SYSCOMMAND()
	ON_WM_PAINT()
	ON_WM_QUERYDRAGICON()
	ON_BN_CLICKED(IDC_BUTTON1, OnButton1)
	ON_BN_CLICKED(IDC_BUTTON2, OnButton2)
	ON_BN_CLICKED(IDC_BUTTON4, OnButton4)
	ON_BN_CLICKED(IDC_BUTTON3, OnButton3)
	//}}AFX_MSG_MAP
END_MESSAGE_MAP()

/////////////////////////////////////////////////////////////////////////////
// CSample1Dlg1 メッセージ ハンドラ

BOOL CSample1Dlg1::OnInitDialog()
{
	CDialog::OnInitDialog();

	// "バージョン情報..." メニュー項目をシステム メニューへ追加します。

	// IDM_ABOUTBOX はコマンド メニューの範囲でなければなりません。
	ASSERT((IDM_ABOUTBOX & 0xFFF0) == IDM_ABOUTBOX);
	ASSERT(IDM_ABOUTBOX < 0xF000);

	CMenu* pSysMenu = GetSystemMenu(FALSE);
	if (pSysMenu != NULL)
	{
		CString strAboutMenu;
		strAboutMenu.LoadString(IDS_ABOUTBOX);
		if (!strAboutMenu.IsEmpty())
		{
			pSysMenu->AppendMenu(MF_SEPARATOR);
			pSysMenu->AppendMenu(MF_STRING, IDM_ABOUTBOX, strAboutMenu);
		}
	}

	// このダイアログ用のアイコンを設定します。フレームワークはアプリケーションのメイン
	// ウィンドウがダイアログでない時は自動的に設定しません。
	SetIcon(m_hIcon, TRUE);			// 大きいアイコンを設定
	SetIcon(m_hIcon, FALSE);		// 小さいアイコンを設定

	//フォント設定
	m_font.CreateFont(12,0,0,0,FW_DONTCARE,FALSE,FALSE,FALSE,SHIFTJIS_CHARSET,
		OUT_DEFAULT_PRECIS,CLIP_DEFAULT_PRECIS,DRAFT_QUALITY,DEFAULT_PITCH,"ＭＳ ゴシック");

	// 各コントロールのフォントを設定
	GetDlgItem(IDC_RICHEDIT1)->SetFont(&m_font);
	GetDlgItem(IDC_RICHEDIT2)->SetFont(&m_font);
	
	// TODO: 特別な初期化を行う時はこの場所に追加してください。

		//メインフォームを指定
		m_pView = this;

		long ReturnCode;                //JVLink戻り値
        CString sid;
        sid = "UNKNOWN";               //引数 JVInit:ソフトウェアID

        //**********************
        //JVLink初期化
        //**********************
        //※※※ JVInitは JVLinkメソッド使用前（但し、JVSetUIProPertiesを除く）に呼出す
        ReturnCode = m_jvlink1.JVInit(sid);

		//文字列に変換
		CString strReturnCode;
		strReturnCode.Format("%d", ReturnCode);

        //エラー判定
        if (ReturnCode != 0)           //エラー
                PrintOut("JVInitエラー:" + strReturnCode + "\xd\xa" );
        else                           //正常
                PrintOut("JVInit正常終了:" + strReturnCode + "\xd\xa" );
		

	return TRUE;  // TRUE を返すとコントロールに設定したフォーカスは失われません。
}

void CSample1Dlg1::OnSysCommand(UINT nID, LPARAM lParam)
{
	if ((nID & 0xFFF0) == IDM_ABOUTBOX)
	{
		CAboutDlg dlgAbout;
		dlgAbout.DoModal();
	}
	else
	{
		CDialog::OnSysCommand(nID, lParam);
	}
}

// もしダイアログボックスに最小化ボタンを追加するならば、アイコンを描画する
// コードを以下に記述する必要があります。MFC アプリケーションは document/view
// モデルを使っているので、この処理はフレームワークにより自動的に処理されます。

void CSample1Dlg1::OnPaint() 
{
	if (IsIconic())
	{
		CPaintDC dc(this); // 描画用のデバイス コンテキスト

		SendMessage(WM_ICONERASEBKGND, (WPARAM) dc.GetSafeHdc(), 0);

		// クライアントの矩形領域内の中央
		int cxIcon = GetSystemMetrics(SM_CXICON);
		int cyIcon = GetSystemMetrics(SM_CYICON);
		CRect rect;
		GetClientRect(&rect);
		int x = (rect.Width() - cxIcon + 1) / 2;
		int y = (rect.Height() - cyIcon + 1) / 2;

		// アイコンを描画します。
		dc.DrawIcon(x, y, m_hIcon);
	}
	else
	{
		CDialog::OnPaint();
	}
}

// システムは、ユーザーが最小化ウィンドウをドラッグしている間、
// カーソルを表示するためにここを呼び出します。
HCURSOR CSample1Dlg1::OnQueryDragIcon()
{
	return (HCURSOR) m_hIcon;
}

//------------------------------------------------------------------------------
//      データ取込みボタンクリック時の処理
//------------------------------------------------------------------------------
void CSample1Dlg1::OnButton1() 
{
	CSample1Dlg2 m_frmjvlinkdlg;
	m_frmjvlinkdlg.DoModal();
		
}

//------------------------------------------------------------------------------
//      「出力」に処理結果を表示
//------------------------------------------------------------------------------
void CSample1Dlg1::PrintOut(CString message)
{
	m_strOut.SetSel(-1,-1);
	m_strOut.ReplaceSel(message);
	HWND hWndCtl;
	hWndCtl = ::GetDlgItem(m_hWnd,IDC_RICHEDIT1);
	::SendMessage( hWndCtl, EM_SCROLL , SB_PAGEDOWN ,0 ) ;
}
//------------------------------------------------------------------------------
//      「読込みファイルリスト」に処理結果を表示
//------------------------------------------------------------------------------
void CSample1Dlg1::PrintFileList(CString message)
{
	m_strFileList.SetSel(-1,-1);
	m_strFileList.ReplaceSel(message);
	HWND hWndCtl;
	hWndCtl = ::GetDlgItem(m_hWnd,IDC_RICHEDIT2);
	::SendMessage( hWndCtl, EM_SCROLL , SB_LINEDOWN ,0 ) ;
}

//------------------------------------------------------------------------------
//　　JVLink設定ウィンドウ表示
//------------------------------------------------------------------------------
void CSample1Dlg1::OnButton2() 
{
	long ReturnCode;

	//**********************
    //JVLink設定画面表示
    //**********************
	ReturnCode=m_jvlink1.JVSetUIProperties();

	//文字列に変換
	CString strReturnCode;
	strReturnCode.Format("%d", ReturnCode);

	if (ReturnCode!=0)
		PrintOut("JVSetUIPropertiesエラー:" + strReturnCode +"\xd\xa" );
	else
		PrintOut("JVSetUIProperties正常終了:" + strReturnCode +"\xd\xa" );
}

//------------------------------------------------------------------------------
//      表示をクリア
//------------------------------------------------------------------------------
void CSample1Dlg1::OnButton4() 
{
	m_strOut.SetWindowText("");
	m_strFileList.SetWindowText("");
}

//------------------------------------------------------------------------------
//      指定したファイルを削除
//------------------------------------------------------------------------------
void CSample1Dlg1::OnButton3() 
{
	CSample1Del m_dlgDel;
	m_dlgDel.DoModal();	
}

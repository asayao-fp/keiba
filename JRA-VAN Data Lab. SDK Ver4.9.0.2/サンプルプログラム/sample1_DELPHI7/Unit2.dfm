object frmJVLinkDialog: TfrmJVLinkDialog
  Left = 363
  Top = 477
  Width = 557
  Height = 247
  Caption = 'Sample1 JVLink Dialog'
  Color = clBtnFace
  Font.Charset = SHIFTJIS_CHARSET
  Font.Color = clWindowText
  Font.Height = -12
  Font.Name = #65325#65331' '#65328#12468#12471#12483#12463
  Font.Style = []
  OldCreateOrder = False
  OnShow = FormShow
  PixelsPerInch = 96
  TextHeight = 12
  object Label1: TLabel
    Left = 16
    Top = 120
    Width = 62
    Height = 12
    Caption = #12480#12454#12531#12525#12540#12489
  end
  object Label2: TLabel
    Left = 16
    Top = 160
    Width = 36
    Height = 12
    Caption = #35501#36796#12415
  end
  object Label3: TLabel
    Left = 16
    Top = 16
    Width = 77
    Height = 12
    Caption = #12501#12449#12452#12523#35672#21029#23376
  end
  object Label4: TLabel
    Left = 248
    Top = 16
    Width = 101
    Height = 12
    Caption = #12487#12540#12479#25552#20379#26085'FROM'
  end
  object ButtonCancel: TButton
    Left = 416
    Top = 72
    Width = 113
    Height = 33
    Caption = #12487#12540#12479#21462#36796#12415#20013#27490
    TabOrder = 5
    OnClick = ButtonCancelClick
  end
  object ProgressBar1: TProgressBar
    Left = 16
    Top = 136
    Width = 377
    Height = 17
    Step = 1
    TabOrder = 2
  end
  object ProgressBar2: TProgressBar
    Left = 16
    Top = 176
    Width = 377
    Height = 17
    Step = 1
    TabOrder = 3
  end
  object txtDataSpec: TEdit
    Left = 16
    Top = 32
    Width = 225
    Height = 20
    TabOrder = 0
  end
  object txtFromDate: TEdit
    Left = 248
    Top = 32
    Width = 145
    Height = 20
    TabOrder = 1
  end
  object ButtonStart: TButton
    Left = 417
    Top = 24
    Width = 113
    Height = 33
    Caption = #12487#12540#12479#21462#36796#12415#38283#22987
    TabOrder = 4
    OnClick = ButtonStartClick
  end
  object GroupBox1: TGroupBox
    Left = 16
    Top = 64
    Width = 361
    Height = 49
    Caption = #21462#24471#12487#12540#12479
    TabOrder = 6
    object rbtNormal: TRadioButton
      Left = 8
      Top = 16
      Width = 105
      Height = 25
      Caption = #36890#24120#12487#12540#12479
      Checked = True
      TabOrder = 0
      TabStop = True
    end
    object rbtIsthisweek: TRadioButton
      Left = 104
      Top = 16
      Width = 105
      Height = 25
      Caption = #20170#36913#38283#20652#12487#12540#12479
      TabOrder = 1
    end
    object rbtSetup: TRadioButton
      Left = 208
      Top = 16
      Width = 121
      Height = 25
      Caption = #12475#12483#12488#12450#12483#12503#12487#12540#12479
      TabOrder = 2
    end
  end
  object TimerJVStatus: TTimer
    Enabled = False
    Interval = 10
    OnTimer = TimerJVStatusTimer
    Left = 424
    Top = 128
  end
end

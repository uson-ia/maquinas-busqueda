import urllib
#enconding: utf-8

#Fuentes
#http://buildsearchengine.blogspot.in/
#https://github.com/dileep98490/A-simple-Search-Engine-in-Python/blob/master/Search_engine.py
max_limit=5



def get_page(url):
#Esta funcion obtiene todo el contenido de una pag web, recivbe el parametro url = web page
  try:
    f = urllib.urlopen(url)
    page = f.read()
    f.close()
    #print page
    return page
  except: 
    return ""
  return ""


  
def union(a,b):
  #Esta funcion agrega el contenido del array b al array a 
  #Por ejemplo si a=[1,2,3] b=[2,3,4].Despues de la union(a,b) quedaria  a=[1,2,3,4] and b=[2,3,4]

  for e in b:
    if e not in a:
      a.append(e)

def get_next_url(page):

  #Primero se busca el string "a href" con la funcion find y el index en donde se localiza el prox "a href" se guarda en 
  #la variable -start_link- si no encuentra el string entonces devuelve un -1
  start_link=page.find("a href")
  print "start_link", start_link
  if(start_link==-1):
    return None,0
  
  #se obtiene el index en donde se encuentra las siguientes comillas  y lo guarda en start_quote
  start_quote=page.find('"',start_link)
  #se obtiene el index en donde se encuentra las siguientes comillas y lo guarda en end_quote
  end_quote=page.find('"',start_quote+1)

  #url:aqui se guarda el prox link de la pag, sin las comillas que se encuentra entre el index page[start_quote] y page[end_quote] 
  url=page[start_quote+1:end_quote]
  print "url",url
  return url,end_quote


def get_all_links(page):
  links=[] #aqui se iran guardando todos los links sin el string "a href" 
  while(True):
  #este ciclo se encarga de ir guardando links de uno por uno,en  cada iteracion guarda un solo link
    url,n=get_next_url(page) #page: es el contenido de la pag, url=es la ultima url del contenido page y n=es el index del doc page en donde se termina el link de esta iteracion
    page=page[n:] #ahora el contenido de page se va cortando de poco a poco por que cada vez se recore desde el indice donde se quedo el ultimo link (n) hasta el final
    if url: #si todavia queda un link en el doc page lo agrega al array de links
      links.append(url)
    else:
      break
  print "aqui son links"
  print links
  return links



def Look_up(index,keyword):
  #Esta funcion recibe un index y una palabra (keyword)  y regresa una lista de los links en donde se encuentra esa palabra

  if keyword in index:
    print "keyword",keyword
    print "batman",index[keyword]
    print "index: ",index
    return index[keyword]
  return []


def add_to_index(index,url,keyword):
#El formato de los elementos en el index is <keyword>,[<lista de urls que contienen la keyword>]
#se van agregando los links que no estan en la lista de links

  if keyword in index:
    if url not in index[keyword]:
      index[keyword].append(url)
    return
  index[keyword]=[url] #guarda la url en el array pero con corchetes
  print "urllllll:",url
  print "[url]:",[url]
def add_page_to_index(index,url,content):#se va agregando el contenido de la pag web al la lista de index
  for i in content.split():
    add_to_index(index,url,i)

def compute_ranks(graph):#recibe un grafo en donde se calcula los ranks  de todos los links
  print "grafos: ",graph
  d=0.8
  numloops=10
  ranks={}
  npages=len(graph)
  for page in graph:
    ranks[page]=1.0/npages
  for i in range(0,numloops):
    newranks={}
    for page in graph:
      newrank=(1-d)/npages
      for node in graph:
        if page in graph[node]:
          newrank=newrank+d*ranks[node]/len(graph[node])
      newranks[page]=newrank
    ranks=newranks
  return ranks
  
def Crawl_web(seed):
  #seed = es el link principal en donde empezaran a buscar los demas links
  tocrawl=[seed]
  crawled=[]
  index={}
  graph={}
  global max_limit
  while tocrawl:
    p=tocrawl.pop()
    if p not in crawled: #si la pag web ya fue analizada por los robots no es necesario volver a analizarla
      max_limit-=1
      print max_limit
      if max_limit<=0:
        break
      c=get_page(p)
      add_page_to_index(index,p,c)
      f=get_all_links(c)
      union(tocrawl,f)
      graph[p]=f
      crawled.append(p)
  return crawled,index,graph #Regresa la lista de todos los links

#print index  



def QuickSort(pages,ranks):#ordena los links de mayor a menor
  if len(pages)>1:
    piv=ranks[pages[0]]
    i=1
    j=1
    for j in range(1,len(pages)):
      if ranks[pages[j]]>piv:
        pages[i],pages[j]=pages[j],pages[i]
        i+=1
    pages[i-1],pages[0]=pages[0],pages[i-1]
    QuickSort(pages[1:i],ranks)
    QuickSort(pages[i+1:len(pages)],ranks)

def Look_up_new(index,ranks,keyword):
  pages=Look_up(index,keyword)
  print '\nPrinting the results as is with page rank\n'
  for i in pages:
    print i+" --> "+str(ranks[i])#Displaying the lists, so that you can see the page rank along side
  QuickSort(pages,ranks)
  print "\nAfter Sorting the results by page rank\n"
  it=0
  for i in pages:#This is how actually it looks like in search engine results - > sorted by page rank
    it+=1
    print str(it)+'.\t'+i+'\n' 


#print index
print "Enter the seed page"
seed_page="http://opencvuser.blogspot.com"
print "Enter What you want to search"
search_term="a"
try:
  print "Enter the depth you wanna go"
  max_limit=int(5)
except:
  f=None
print '\nStarted crawling, presently at depth..'
crawled,index,graph=Crawl_web(seed_page) #imprime todos los links

ranks=compute_ranks(graph)#Calcula el rank the cada pag web
Look_up_new(index,ranks,search_term)

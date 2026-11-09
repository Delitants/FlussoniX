export function watchTableOverflow(root:Element):()=>void{
 const observed=new Set<Element>();
 let frame=0;
 const resize=new ResizeObserver(()=>schedule());
 function update(){
  frame=0;
  const current=new Set<Element>();
  root.querySelectorAll<HTMLElement>('.table-wrap').forEach(wrap=>{
   const table=wrap.querySelector('table');
   if(!table)return;
   current.add(wrap);current.add(table);
   for(const element of [wrap,table])if(!observed.has(element)){resize.observe(element);observed.add(element)}
   wrap.classList.toggle('has-table-overflow',table.scrollWidth>wrap.clientWidth+1);
  });
  root.querySelectorAll<HTMLElement>('.status-strip,.tabs.mobile-cue-detail,.tabs.mobile-cue-cluster').forEach(strip=>{
   current.add(strip);
   if(!observed.has(strip)){resize.observe(strip);observed.add(strip)}
   for(const child of Array.from(strip.children)){
    current.add(child);
    if(!observed.has(child)){resize.observe(child);observed.add(child)}
   }
   // Exclude the cue itself from the width check: its sticky flex item can
   // otherwise create the overflow that keeps it visible.
   strip.classList.remove('has-inline-overflow');
   strip.classList.toggle('has-inline-overflow',strip.scrollWidth>strip.clientWidth+1);
  });
  for(const element of observed)if(!current.has(element)){resize.unobserve(element);observed.delete(element)}
 }
 function schedule(){if(!frame)frame=requestAnimationFrame(update)}
 const mutations=new MutationObserver(schedule);
 mutations.observe(root,{childList:true,subtree:true,characterData:true});
 schedule();
 return ()=>{mutations.disconnect();resize.disconnect();if(frame)cancelAnimationFrame(frame)};
}
